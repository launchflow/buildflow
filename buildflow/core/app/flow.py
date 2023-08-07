import asyncio
import dataclasses
import inspect
import logging
import os
import signal
from typing import List, Optional

from ray import serve

from buildflow.config.buildflow_config import BuildFlowConfig
from buildflow.core import utils
from buildflow.core.app.infra.actors.infra import InfraActor
from buildflow.core.app.infra.pulumi_workspace import PulumiWorkspace, WrappedStackState
from buildflow.core.app.runtime._runtime import RunID
from buildflow.core.app.runtime.actors.runtime import RuntimeActor
from buildflow.core.app.runtime.server import RuntimeServer
from buildflow.core.background_tasks.background_task import BackgroundTask
from buildflow.core.credentials._credentials import CredentialType
from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.core.credentials.empty_credentials import EmptyCredentials
from buildflow.core.credentials.gcp_credentials import GCPCredentials
from buildflow.core.io.primitive import PortablePrimtive, Primitive, PrimitiveType
from buildflow.core.options.flow_options import FlowOptions
from buildflow.core.options.runtime_options import ProcessorOptions
from buildflow.core.processor.patterns.pipeline import PipelineProcessor
from buildflow.core.processor.processor import ProcessorAPI
from buildflow.core.strategies._strategy import StategyType
from buildflow.io.local.empty import Empty


def _get_directory_path_of_caller():
    # NOTE: This function is used to get the file path of the caller of the
    # Flow(). This is used to determine the directory to look for a BuildFlow
    # Config.
    frame = inspect.stack()[2]
    module = inspect.getmodule(frame[0])
    return os.path.dirname(os.path.abspath(module.__file__))


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
            "pulumi_stack_state": self.pulumi_stack_state.as_json_dict(),
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
        # NOTE: we use a list here instead of a set because we have no
        # guarantee that primitives will be cachable.
        self._primitive_cache: List[Primitive] = []

    def _get_infra_actor(self) -> InfraActor:
        if self._infra_actor_ref is None:
            self._infra_actor_ref = InfraActor(
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

    def _get_credentials(self, primitive_type: PrimitiveType):
        if primitive_type == PrimitiveType.GCP:
            return GCPCredentials(self.options.credentials_options)
        elif primitive_type == PrimitiveType.AWS:
            return AWSCredentials(self.options.credentials_options)
        return EmptyCredentials(self.options.credentials_options)

    def _portable_primitive_to_cloud_primitive(
        self, primitive: Primitive, strategy_type: StategyType
    ):
        if primitive.primitive_type == PrimitiveType.PORTABLE:
            primitive: PortablePrimtive
            primitive = primitive.to_cloud_primitive(
                cloud_provider_config=self.config.cloud_provider_config,
                strategy_type=strategy_type,
            )
            primitive.enable_managed()
        return primitive

    def _background_tasks(
        self, primitive: Primitive, credentials: CredentialType
    ) -> List[BackgroundTask]:
        provider = primitive.background_task_provider()
        if provider is not None:
            return provider.background_tasks(credentials)
        return []

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
            sink = Empty()

        # Convert any Portableprimitives into cloud-specific primitives
        source = self._portable_primitive_to_cloud_primitive(source, StategyType.SOURCE)
        sink = self._portable_primitive_to_cloud_primitive(sink, StategyType.SINK)

        # Set up credentials
        source_credentials = self._get_credentials(source.primitive_type)
        sink_credentials = self._get_credentials(sink.primitive_type)

        return self._pipeline_decorator(
            source_primitive=source,
            sink_primitive=sink,
            processor_options=ProcessorOptions(
                num_cpus=num_cpus,
                num_concurrency=num_concurrency,
                log_level=log_level,
            ),
            source_credentials=source_credentials,
            sink_credentials=sink_credentials,
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
        await self._get_infra_actor().plan(processors=self._processors)
        logging.debug(f"...Finished planning Infra for Flow({self.flow_id})")

    def apply(self):
        return asyncio.get_event_loop().run_until_complete(self._apply())

    async def _apply(self):
        logging.debug(f"Setting up Infra for Flow({self.flow_id})...")
        await self._get_infra_actor().apply(processors=self._processors)
        logging.debug(f"...Finished setting up Infra for Flow({self.flow_id})")

    def destroy(self):
        return asyncio.get_event_loop().run_until_complete(self._destroy())

    async def _destroy(self):
        logging.debug(f"Tearing down infrastructure for Flow({self.flow_id})...")
        await self._get_infra_actor().destroy(processors=self._processors)
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

    def _pipeline_decorator(
        self,
        source_primitive: Primitive,
        sink_primitive: Optional[Primitive],
        processor_options: ProcessorOptions,
        source_credentials: CredentialType,
        sink_credentials: CredentialType,
    ):
        def decorator_function(original_process_fn_or_class):
            if inspect.isclass(original_process_fn_or_class):

                def setup(self):
                    if hasattr(self.instance, "setup"):
                        self.instance.setup()

                async def teardown(self):
                    coros = []
                    if hasattr(self.instance, "teardown"):
                        if inspect.iscoroutinefunction(self.instance.teardown()):
                            coros.append(self.instance.teardown())
                        else:
                            self.instance.teardown()
                    coros.extend([self.source().teardown(), self.sink().teardown()])
                    await asyncio.gather(*coros)

                full_arg_spec = inspect.getfullargspec(
                    original_process_fn_or_class.process
                )
            else:

                def setup(self):
                    return None

                async def teardown(self):
                    await asyncio.gather(
                        self.source().teardown(), self.sink().teardown()
                    )

                full_arg_spec = inspect.getfullargspec(original_process_fn_or_class)
            input_type = None
            output_type = None
            if (
                len(full_arg_spec.args) > 1
                and full_arg_spec.args[1] in full_arg_spec.annotations
            ):
                input_type = full_arg_spec.annotations[full_arg_spec.args[1]]
            if "return" in full_arg_spec.annotations:
                output_type = full_arg_spec.annotations["return"]

            # NOTE: We only create Pulumi resources for managed primitives and only
            # for the first time we see a primitive.
            include_source_primitive = False
            if source_primitive not in self._primitive_cache:
                self._primitive_cache.append(source_primitive)
                include_source_primitive = True
            include_sink_primitive = False
            if sink_primitive not in self._primitive_cache:
                self._primitive_cache.append(sink_primitive)
                include_sink_primitive = True

            def pulumi_resources():
                all_resources = []
                # Builds the source's pulumi.CompositeResource (if it exists)
                if source_primitive.managed and include_source_primitive:
                    source_pulumi_provider = source_primitive.pulumi_provider()
                    source_resource = source_pulumi_provider.pulumi(
                        type_=input_type,
                        credentials=source_credentials,
                    )
                    if source_resource is not None:
                        all_resources.append(source_resource)
                # Builds the sink's pulumi.CompositeResource (if it exists)
                if sink_primitive.managed and include_sink_primitive:
                    sink_pulumi_provider = sink_primitive.pulumi_provider()
                    sink_resource = sink_pulumi_provider.pulumi(
                        type_=output_type, credentials=sink_credentials
                    )
                    if sink_resource is not None:
                        all_resources.append(sink_resource)
                return all_resources

            def background_tasks():
                return self._background_tasks(
                    source_primitive, source_credentials
                ) + self._background_tasks(sink_primitive, sink_credentials)

            # Dynamically define a new class with the same structure as Processor
            processor_id = original_process_fn_or_class.__name__
            class_name = f"PipelineProcessor{utils.uuid(max_len=8)}"
            source_provider = source_primitive.source_provider()
            sink_provider = sink_primitive.sink_provider()
            adhoc_methods = {
                # PipelineProcessor methods.
                # NOTE: We need to instantiate the source and sink strategies
                # in the class to avoid issues passing to ray workers.
                "source": lambda self: source_provider.source(source_credentials),
                "sink": lambda self: sink_provider.sink(sink_credentials),
                # ProcessorAPI methods. NOTE: process() is attached separately below
                "resources": lambda self: pulumi_resources(),
                "setup": setup,
                "teardown": teardown,
                "background_tasks": lambda self: background_tasks(),
                "__meta__": {
                    "source": source_primitive,
                    "sink": sink_primitive,
                },
                "__call__": original_process_fn_or_class,
            }
            if inspect.isclass(original_process_fn_or_class):

                def init_processor(self, processor_id):
                    self.processor_id = processor_id
                    self.instance = original_process_fn_or_class()

                adhoc_methods["__init__"] = init_processor
            AdHocPipelineProcessorClass = type(
                class_name,
                (PipelineProcessor,),
                adhoc_methods,
            )
            if not inspect.isclass(original_process_fn_or_class):
                utils.attach_method_to_class(
                    AdHocPipelineProcessorClass,
                    "process",
                    original_func=original_process_fn_or_class,
                )
            else:
                utils.attach_wrapped_method_to_class(
                    AdHocPipelineProcessorClass,
                    "process",
                    original_func=original_process_fn_or_class.process,
                )

            processor = AdHocPipelineProcessorClass(processor_id=processor_id)
            self.add_processor(processor, processor_options)

            return processor

        return decorator_function
