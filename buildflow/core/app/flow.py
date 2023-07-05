import asyncio
import logging
import signal
from typing import List, Optional, Type, Union

from buildflow import utils
from buildflow.api.primitives._primitive import PrimitiveAPI
from buildflow.api.primitives.empty import Empty
from buildflow.core.app.infra.actors.infra import InfraActor
from buildflow.core.app.runtime.actors.runtime import RuntimeActor, RuntimeSnapshot
from buildflow.core.io.providers._provider import PulumiResources
from buildflow.core.io.registry import Registry
from buildflow.core.io.resources._resource import Resource
from buildflow.core.options.flow_options import FlowOptions
from buildflow.core.options.runtime_options import ProcessorOptions
from buildflow.core.io.providers._provider import (
    ProcessorProvider,
    SinkProvider,
    SourceProvider,
)
from buildflow.api.patterns.processor import Processor
from buildflow.core.io.providers._provider import Provider
import inspect
import copy


# NEXT TIME - figure out how to avoid using context vars for source_provider and sink_provider.
# It might not be possible to pass in the providers here. We probably need to pass the resources and
# then lookup the providers using the registry


def processor_decorator(
    flow: "Flow",
    source_provider: SourceProvider,
    sink_provider: SinkProvider,
    processor_options: ProcessorOptions,
):
    def decorator_function(original_process_function):
        processor_id = original_process_function.__name__
        # Dynamically define a new class with the same structure as Processor
        class_name = f"AdHocProcessor_{utils.uuid(max_len=8)}"
        _AdHocProcessor = type(
            class_name,
            (Processor,),
            {
                # Processor methods. NOTE: process() is attached separately below
                "source": lambda self: source_provider.source(),
                "sink": lambda self: sink_provider.sink(),
                "setup": lambda self: None,
            },
        )
        utils.attach_method_to_class(
            _AdHocProcessor, "process", original_process_function
        )

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

        def pulumi_resources(self, type_: Optional[Type]):
            source_resources = source_provider.pulumi_resources(input_type)
            sink_resources = sink_provider.pulumi_resources(output_type)
            return PulumiResources.merge(source_resources, sink_resources)

        _AdHocProcessorProvider = type(
            f"{class_name}Provider",
            (ProcessorProvider,),
            {
                # ProcessorProvider attributes
                "processor_id": processor_id,
                # ProcessorProvider methods
                "processor": lambda self: _AdHocProcessor(processor_id=processor_id),
                "pulumi_resources": pulumi_resources,
                # Returns the original function when called
                "__call__": wrapper_function,
            },
        )

        processor_provider: ProcessorProvider = _AdHocProcessorProvider()
        flow.add_processor(processor_provider, processor_options)

        return processor_provider

    return decorator_function


FlowID = str


class Flow:
    def __init__(
        self,
        flow_id: FlowID = "buildflow-app",
        flow_options: Optional[FlowOptions] = None,
    ) -> None:
        # Flow configuration
        self.flow_id = flow_id
        self.options = flow_options or FlowOptions.default()
        # Flow initial state
        self._processors: List[ProcessorProvider] = []

        # Registry configuration.
        # TODO: Should this be created in the Runner and passed in?
        self._registry = Registry(self.options.resource_options)

        # Runtime configuration
        self._runtime_actor_ref: Optional[RuntimeActor] = None

        # Infra configuration
        self._infra_actor_ref: Optional[InfraActor] = None

    @property
    def _infra_actor(self) -> InfraActor:
        if self._infra_actor_ref is None:
            self._infra_actor_ref = InfraActor.remote(
                infra_options=self.options.infra_options
            )
        return self._infra_actor_ref

    @property
    def _runtime_actor(self) -> RuntimeActor:
        if self._runtime_actor_ref is None:
            self._runtime_actor_ref = RuntimeActor.remote(
                runtime_options=self.options.runtime_options,
            )
        return self._runtime_actor_ref

    # NOTE: The Flow class is responsible for converting Resources / Primitives into a
    # Provider.
    def processor(
        self,
        source: Union[Resource, PrimitiveAPI],
        sink: Optional[Union[Resource, PrimitiveAPI]] = None,
        *,
        num_cpus: float = 1.0,
        num_concurrency: int = 1,
        log_level: str = "INFO",
    ):
        def get_source_provider_from_registry(
            source: Union[Resource, PrimitiveAPI]
        ) -> SourceProvider:
            source_provider: Optional[SourceProvider] = None
            if isinstance(source, PrimitiveAPI):
                source_provider = self._registry.get_source_provider_for_primitive(
                    source
                )
            elif isinstance(source, Resource):
                source_provider = self._registry.get_source_provider_for_resource(
                    source
                )
            else:
                raise ValueError(f"Unsupported source type: {type(source)}")
            return source_provider

        def get_sink_provider_from_registry(
            sink: Optional[Union[Resource, PrimitiveAPI]]
        ) -> SinkProvider:
            sink_provider: Optional[SinkProvider] = None
            if sink is None:
                sink_provider = self._registry.get_sink_provider_for_primitive(Empty())
            elif isinstance(sink, PrimitiveAPI):
                sink_provider = self._registry.get_sink_provider_for_primitive(sink)
            elif isinstance(sink, Resource):
                sink_provider = self._registry.get_sink_provider_for_resource(sink)
            else:
                raise ValueError(f"Unsupported sink type: {type(sink)}")
            return sink_provider

        source_provider = get_source_provider_from_registry(source)
        sink_provider = get_sink_provider_from_registry(sink)

        # NOTE: processor_decorator returns a ProcessorProvider
        return processor_decorator(
            flow=self,
            source_provider=source_provider,
            sink_provider=sink_provider,
            processor_options=ProcessorOptions(
                num_cpus=num_cpus,
                num_concurrency=num_concurrency,
                log_level=log_level,
            ),
        )

    def add_processor(
        self,
        processor: ProcessorProvider,
        options: Optional[ProcessorOptions] = None,
    ):
        if self._runtime_actor_ref is not None or self._infra_actor_ref is not None:
            raise RuntimeError(
                "Cannot add processor to an active Flow. Did you already call run()?"
            )
        if processor.processor_id in self.options.runtime_options.processor_options:
            raise RuntimeError(
                f"Processor({processor.processor_id}) already exists in Flow."
            )
        # Each processor gets its own replica config
        self.options.runtime_options.processor_options[processor.processor_id] = options
        self._processors.append(processor)

    async def run(self):
        # Add a signal handler to drain the runtime when the process is killed
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig,
                lambda: asyncio.create_task(self.drain()),
            )
        # start the runtime
        await self._runtime_actor.run.remote(processors=self._processors)

    async def run_until_complete(self):
        remote_runtime_task = self._runtime_actor.run_until_complete.remote()
        return await remote_runtime_task

    async def drain(self):
        logging.debug(f"Draining Flow({self.flow_id})...")
        await self._runtime_actor.drain.remote()
        logging.debug(f"...Finished draining Flow({self.flow_id})")
        return True

    # TODO: Add Infra snapshots and make this a FlowSnapshot
    async def snapshot(self) -> RuntimeSnapshot:
        logging.debug(f"Taking snapshot of Flow({self.flow_id})...")
        snapshot: RuntimeSnapshot = await self._runtime_actor.snapshot.remote()
        logging.debug(f"...Finished taking snapshot of Flow({self.flow_id})")
        return snapshot

    async def plan(self):
        logging.debug(f"Planning Infra for Flow({self.flow_id})...")
        await self._infra_actor.plan.remote(providers=self._processors)
        logging.debug(f"...Finished planning Infra for Flow({self.flow_id})")

    async def apply(self):
        logging.debug(f"Setting up Infra for Flow({self.flow_id})...")
        await self._infra_actor.apply.remote(providers=self._processors)
        logging.debug(f"...Finished setting up Infra for Flow({self.flow_id})")

    async def destroy(self):
        logging.debug(f"Tearing down infrastructure for Flow({self.flow_id})...")
        await self._infra_actor.destroy.remote(providers=self._processors)
        logging.debug(
            f"...Finished tearing down infrastructure for Flow({self.flow_id})"
        )
