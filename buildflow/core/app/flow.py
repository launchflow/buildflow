import asyncio
import dataclasses
import inspect
import logging
import os
import signal
from typing import List, Optional, Type

from ray import serve

from buildflow.config.buildflow_config import BuildFlowConfig
from buildflow.core import utils
from buildflow.core.app.infra.actors.infra import InfraActor
from buildflow.core.app.infra.pulumi_workspace import PulumiWorkspace, WrappedStackState
from buildflow.core.app.runtime._runtime import RunID
from buildflow.core.app.runtime.actors.runtime import RuntimeActor
from buildflow.core.app.runtime.server import RuntimeServer
from buildflow.core.io.primitive import (
    EmptyPrimitive,
    PortablePrimtive,
    Primitive,
    PrimitiveType,
)
from buildflow.core.options.flow_options import FlowOptions
from buildflow.core.options.runtime_options import ProcessorOptions
from buildflow.core.processor.patterns.pipeline import PipelineProcessor
from buildflow.core.processor.processor import ProcessorAPI
from buildflow.core.strategies._stategy import StategyType


def _get_directory_path_of_caller():
    # NOTE: This function is used to get the file path of the caller of the
    # Flow(). This is used to determine the directory to look for a BuildFlow
    # Config.
    frame = inspect.stack()[2]
    module = inspect.getmodule(frame[0])
    return os.path.dirname(os.path.abspath(module.__file__))


def pipeline_decorator(
    flow: "Flow",
    source_primitive: Primitive,
    sink_primitive: Primitive,
    processor_options: ProcessorOptions,
):
    def decorator_function(original_process_function):
        def wrapper_function(*args, **kwargs):
            return original_process_function(*args, **kwargs)

        full_arg_spec = inspect.getfullargspec(original_process_function)
        input_type = None
        output_type = None
        if (
            len(full_arg_spec.args) > 1
            and full_arg_spec.args[1] in full_arg_spec.annotations
        ):
            input_type = full_arg_spec.annotations[full_arg_spec.args[1]]
        if "return" in full_arg_spec.annotations:
            output_type = full_arg_spec.annotations["return"]

        # NOTE: We only create Pulumi resources for managed primitives
        # (Currently only PortablePrimitives)
        def pulumi_resources(type_: Optional[Type] = None):
            source_resources = []
            if source_primitive.managed:
                source_pulumi_provider = source_primitive.pulumi_provider()
                source_resources = source_pulumi_provider.pulumi_resources(input_type)
            sink_resources = []
            if sink_primitive.managed:
                sink_pulumi_provider = sink_primitive.pulumi_provider()
                sink_resources = sink_pulumi_provider.pulumi_resources(output_type)
            return source_resources + sink_resources

        # Dynamically define a new class with the same structure as Processor
        source_provider = source_primitive.source_provider()
        sink_provider = sink_primitive.sink_provider()
        processor_id = original_process_function.__name__
        class_name = f"PipelineProcessor{utils.uuid(max_len=8)}"
        AdHocPipelineProcessorClass = type(
            class_name,
            (PipelineProcessor,),
            {
                # PipelineProcessor methods.
                "source": lambda self: source_provider.source(),
                "sink": lambda self: sink_provider.sink(),
                # ProcessorAPI methods. NOTE: process() is attached separately below
                "resources": lambda self: pulumi_resources(),
                "setup": lambda self: None,
                "__meta__": {
                    "source": source_primitive,
                    "sink": sink_primitive,
                },
                "__call__": wrapper_function,
            },
        )
        utils.attach_method_to_class(
            AdHocPipelineProcessorClass, "process", original_process_function
        )

        processor = AdHocPipelineProcessorClass(processor_id=processor_id)
        flow.add_processor(processor, processor_options)

        return processor

    return decorator_function


FlowID = str


@dataclasses.dataclass
class FlowState:
    flow_id: FlowID
    processors: List[ProcessorAPI]
    pulumi_stack_state: WrappedStackState

    def as_json_dict(self):
        return {
            "flow_id": self.flow_id,
            "processors": [
                {
                    "processor_id": p.processor_id,
                    "processor_type": p.processor_type.value,
                    "meta": p.__meta__,
                }
                for p in self.processors
            ],
        }


class Flow:
    def __init__(
        self,
        flow_id: FlowID = "buildflow-app",
        flow_options: Optional[FlowOptions] = None,
    ) -> None:
        # Load the BuildFlow Config to get the default options
        buildflow_config_dir = os.path.join(
            _get_directory_path_of_caller(), ".buildflow"
        )
        self.config = BuildFlowConfig.create_or_load(buildflow_config_dir)
        # Flow configuration
        self.flow_id = flow_id
        self.options = flow_options or FlowOptions.default()
        # Flow initial state
        self._processors: List[ProcessorAPI] = []
        # Runtime configuration
        self._runtime_actor_ref: Optional[RuntimeActor] = None
        # Infra configuration
        self._infra_actor_ref: Optional[InfraActor] = None

    def _get_infra_actor(self) -> InfraActor:
        if self._infra_actor_ref is None:
            self._infra_actor_ref = InfraActor.remote(
                infra_options=self.options.infra_options,
                pulumi_config=self.config.pulumi_config,
            )
        return self._infra_actor_ref

    def _get_runtime_actor(self, run_id: Optional[RunID] = None) -> RuntimeActor:
        if self._runtime_actor_ref is None:
            if run_id is None:
                run_id = utils.uuid()
            self._runtime_actor_ref = RuntimeActor.remote(
                run_id=run_id,
                runtime_options=self.options.runtime_options,
            )
        return self._runtime_actor_ref

    # NOTE: The Flow class is responsible for converting Primitives into a Provider
    def pipeline(
        self,
        source: Primitive,
        sink: Optional[Primitive] = None,
        *,
        num_cpus: float = 1.0,
        num_concurrency: int = 1,
        log_level: str = "INFO",
    ):
        if sink is None:
            sink = EmptyPrimitive()

        # Convert any Portableprimitives into cloud-specific primitives
        if source.primitive_type == PrimitiveType.PORTABLE:
            source: PortablePrimtive
            source = source.to_cloud_primitive(
                cloud_provider_config=self.config.cloud_provider_config,
                strategy_type=StategyType.SOURCE,
            )
            source.enable_managed()

        if sink.primitive_type == PrimitiveType.PORTABLE:
            sink: PortablePrimtive
            sink = sink.to_cloud_primitive(
                cloud_provider_config=self.config.cloud_provider_config,
                strategy_type=StategyType.SINK,
            )
            sink.enable_managed()

        return pipeline_decorator(
            flow=self,
            source_primitive=source,
            sink_primitive=sink,
            processor_options=ProcessorOptions(
                num_cpus=num_cpus,
                num_concurrency=num_concurrency,
                log_level=log_level,
            ),
        )

    def add_processor(
        self,
        processor: ProcessorAPI,
        options: Optional[ProcessorOptions] = None,
    ):
        if self._runtime_actor_ref is not None or self._infra_actor_ref is not None:
            raise RuntimeError(
                "Cannot add processor to an active Flow. Did you already call run()?"
            )
        if processor.processor_id in self.options.runtime_options.processor_options:
            raise RuntimeError(
                f"Processor({processor.processor_id}) already exists in Flow object."
                "Please rename your function or remove the other Processor."
            )
        # Each processor gets its own replica config
        self.options.runtime_options.processor_options[processor.processor_id] = options
        self._processors.append(processor)

    def run(
        self,
        *,
        disable_usage_stats: bool = False,
        block: bool = True,
        # runtime-only options
        debug_run: bool = False,
        run_id: Optional[RunID] = None,
        # server-only options. TODO: Move this into RuntimeOptions / consider
        # having Runtime manage the server
        start_runtime_server: bool = False,
        runtime_server_host: str = "127.0.0.1",
        runtime_server_port: int = 9653,
    ):
        # BuildFlow Usage Stats
        if not disable_usage_stats:
            utils.log_buildflow_usage()

        # Start the Flow Runtime
        runtime_coroutine = self._run(debug_run=debug_run)

        # Start the Runtime Server (maybe)
        if start_runtime_server:
            runtime_server = RuntimeServer.bind(
                runtime_actor=self._get_runtime_actor(run_id=run_id)
            )
            serve.run(
                runtime_server,
                host=runtime_server_host,
                port=runtime_server_port,
            )
            server_log_message = (
                "-" * 80
                + "\n\n"
                + f"Runtime Server running at http://{runtime_server_host}:{runtime_server_port}\n\n"
                + "-" * 80
                + "\n\n"
            )
            logging.info(server_log_message)
            print(server_log_message)

        # Block until the Flow Runtime is finished (maybe)
        if block:
            asyncio.get_event_loop().run_until_complete(runtime_coroutine)
        else:
            return runtime_coroutine

    async def _run(self, debug_run: bool = False):
        # Add a signal handler to drain the runtime when the process is killed
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig,
                lambda: asyncio.create_task(self._drain()),
            )
        # start the runtime and await it to finish
        # NOTE: run() does not necessarily block until the runtime is finished.
        await self._get_runtime_actor().run.remote(processors=self._processors)
        await self._get_runtime_actor().run_until_complete.remote()

    async def _drain(self):
        logging.debug(f"Draining Flow({self.flow_id})...")
        await self._get_runtime_actor().drain.remote()
        logging.debug(f"...Finished draining Flow({self.flow_id})")
        return True

    def plan(self):
        return asyncio.get_event_loop().run_until_complete(self._plan())

    async def _plan(self):
        logging.debug(f"Planning Infra for Flow({self.flow_id})...")
        await self._get_infra_actor().plan.remote(processors=self._processors)
        logging.debug(f"...Finished planning Infra for Flow({self.flow_id})")

    def apply(self):
        return asyncio.get_event_loop().run_until_complete(self._apply())

    async def _apply(self):
        logging.debug(f"Setting up Infra for Flow({self.flow_id})...")
        await self._get_infra_actor().apply.remote(processors=self._processors)
        logging.debug(f"...Finished setting up Infra for Flow({self.flow_id})")

    def destroy(self):
        return asyncio.get_event_loop().run_until_complete(self._destroy())

    async def _destroy(self):
        logging.debug(f"Tearing down infrastructure for Flow({self.flow_id})...")
        await self._get_infra_actor().destroy.remote(processors=self._processors)
        logging.debug(
            f"...Finished tearing down infrastructure for Flow({self.flow_id})"
        )

    def inspect(self):
        pulumi_workspace = PulumiWorkspace(
            pulumi_options=self.options.infra_options.pulumi_options,
            pulumi_config=self.config.pulumi_config,
        )
        pulumi_stack_state = pulumi_workspace.get_stack_state()
        return FlowState(
            flow_id=self.flow_id,
            processors=self._processors,
            pulumi_stack_state=pulumi_stack_state,
        )
