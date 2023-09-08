import asyncio
import dataclasses
import datetime
import inspect as type_inspect
import logging
import os
import signal
from typing import Callable, Dict, List, Optional, Set, Tuple

import pulumi

from buildflow.config.buildflow_config import BuildFlowConfig
from buildflow.core import utils
from buildflow.core.app.collector import Collector
from buildflow.core.app.consumer import Consumer
from buildflow.core.app.endpoint import Endpoint
from buildflow.core.app.infra.actors.infra import InfraActor
from buildflow.core.app.infra.pulumi_workspace import PulumiWorkspace, ResourceState
from buildflow.core.app.runtime._runtime import RunID
from buildflow.core.app.runtime.actors.runtime import RuntimeActor
from buildflow.core.app.runtime.server import RuntimeServer
from buildflow.core.app.service import Service
from buildflow.core.background_tasks.background_task import BackgroundTask
from buildflow.core.credentials._credentials import CredentialType
from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.core.credentials.empty_credentials import EmptyCredentials
from buildflow.core.credentials.gcp_credentials import GCPCredentials
from buildflow.core.options.flow_options import FlowOptions
from buildflow.core.options.runtime_options import AutoscalerOptions, ProcessorOptions
from buildflow.core.processor.patterns.collector import (
    CollectorGroup,
    CollectorProcessor,
)
from buildflow.core.processor.patterns.consumer import ConsumerGroup, ConsumerProcessor
from buildflow.core.processor.patterns.endpoint import EndpointGroup, EndpointProcessor
from buildflow.core.processor.processor import (
    ProcessorAPI,
    ProcessorGroup,
    ProcessorID,
    ProcessorType,
)
from buildflow.io.endpoint import RouteInfo, Method, Route
from buildflow.io.local.empty import Empty
from buildflow.io.primitive import PortablePrimtive, Primitive, PrimitiveType
from buildflow.io.strategies._strategy import StategyType


@dataclasses.dataclass
class _PrimitiveCacheEntry:
    primitive: Primitive
    pulumi_resource: pulumi.Resource


@dataclasses.dataclass
class _PrimitiveCache:
    cache: List[_PrimitiveCacheEntry] = dataclasses.field(default_factory=list)

    def __contains__(self, primitive: Primitive):
        return self.get(primitive) is not None

    def append(self, entry: _PrimitiveCacheEntry):
        self.cache.append(entry)

    def get(self, primitive: Primitive):
        for entry in self.cache:
            if entry.primitive == primitive:
                return entry.pulumi_resource
        return None

    def clear(self):
        self.cache.clear()


def _get_directory_path_of_caller():
    # NOTE: This function is used to get the file path of the caller of the
    # Flow(). This is used to determine the directory to look for a BuildFlow
    # Config.
    frame = type_inspect.stack()[2]
    module = type_inspect.getmodule(frame[0])
    return os.path.dirname(os.path.abspath(module.__file__))


FlowID = str


def _traverse_primitive_for_pulumi(
    primitive: Primitive,
    credentials: CredentialType,
    initial_opts: pulumi.ResourceOptions,
    visited_primitives: _PrimitiveCache,
) -> pulumi.Resource:
    pulumi_provider = primitive.pulumi_provider()
    if pulumi_provider is None:
        raise ValueError(
            "_traverse_primitive_for_pulumi should never be called with "
            "an unmanaged primitive."
        )
    fields = dataclasses.fields(primitive)
    parent_resources = []
    for field in fields:
        field_value = getattr(primitive, field.name)
        if (
            field_value is not None
            and isinstance(field_value, Primitive)
            and field_value.pulumi_provider() is not None
        ):
            visited_resource = visited_primitives.get(field_value)
            # Visit all managed parent primitives to create the parent resources
            if visited_resource is None:
                parent_resources.append(
                    _traverse_primitive_for_pulumi(
                        field_value,
                        credentials,
                        initial_opts,
                        visited_primitives,
                    )
                )
            else:
                parent_resources.append(visited_resource)

    opts = pulumi.ResourceOptions.merge(
        initial_opts, pulumi.ResourceOptions(depends_on=parent_resources)
    )
    resource = pulumi_provider.pulumi_resource(credentials=credentials, opts=opts)
    visited_primitives.append(_PrimitiveCacheEntry(primitive, resource))
    return resource


def _find_primitives_with_no_parents(primitives: List[Primitive]) -> List[Primitive]:
    primitives_without_parents = primitives.copy()
    for primitive in primitives:
        fields = dataclasses.fields(primitive)
        for field in fields:
            field_value = getattr(primitive, field.name)
            if (
                field_value is not None
                and isinstance(field_value, Primitive)
                and field_value.pulumi_provider() is not None
            ):
                primitives_without_parents.remove(field_value)
    return primitives_without_parents


@dataclasses.dataclass
class PrimitiveState:
    primitive_class: str
    resources: List[ResourceState]

    def as_json_dict(self):
        return {
            "primitive_class": self.primitive_class,
            "resources": [r.as_json_dict() for r in self.resources],
        }


@dataclasses.dataclass
class ProcessorState:
    processor_id: ProcessorID
    processor_type: ProcessorType
    source: Optional[PrimitiveState]
    sink: Optional[PrimitiveState]

    def as_json_dict(self):
        return {
            "processor_id": self.processor_id,
            "processor_type": self.processor_type,
            "source": self.source.as_json_dict() if self.source else None,
            "sink": self.sink.as_json_dict() if self.sink else None,
        }


@dataclasses.dataclass
class FlowState:
    flow_id: FlowID
    processors: List[ProcessorState]
    untracked_resources: List[ResourceState]
    last_updated: datetime.datetime
    num_pulumi_resources: int
    pulumi_stack_name: str

    @classmethod
    def parse_resource_states(
        cls,
        flow_id: FlowID,
        processor_refs: List[ProcessorAPI],
        resource_states: List[ResourceState],
        last_updated: datetime.datetime,
        pulumi_stack_name: str,
    ):
        def find_attached_resources(
            parent_resource: ResourceState,
        ) -> Dict[str, ResourceState]:
            """Find all attached resources for a given URN."""
            resources = {}
            dependencies = []
            for resource in resource_states:
                if resource.parent == parent_resource.resource_urn:
                    if (
                        resource.cloud_console_url is None
                        and parent_resource.cloud_console_url is not None
                    ):
                        resource.cloud_console_url = parent_resource.cloud_console_url
                    resources[resource.resource_urn] = resource
                if resource.resource_urn in parent_resource.dependencies:
                    dependencies.append(resource)

            for dependency in dependencies:
                for resource in resource_states:
                    if dependency.parent == resource.resource_urn:
                        resources.update(find_attached_resources(resource))

            return resources

        def find_processor_resource(processor_id: str) -> Optional[ResourceState]:
            """Find the resource for a given processor_id."""
            # TODO: need to find a different way to do this
            for resource in resource_states:
                if (
                    resource.resource_type == "buildflow:processor:Consumer"
                    or resource.resource_type == "buildflow:processor:Collector"
                    or resource.resource_type == "buildflow:processor:Endpoint"
                ):
                    if resource.resource_outputs.get("processor_id") == processor_id:
                        return resource
            return None

        processors: List[ProcessorState] = []
        tracked_resources: Set[str] = set()
        num_pulumi_resources = 0

        # Store the resources in a dict keyed by URN for quick lookup
        resource_dict = {
            resource.resource_urn: resource for resource in resource_states
        }

        for processor_ref in processor_refs:
            # Get the processor_id and processor_type
            processor_id = processor_ref.processor_id
            processor_type = processor_ref.processor_type.value
            # Look up the source and sink class names
            source_class = ""
            source_primitive = processor_ref.__meta__.get("source")
            if source_primitive:
                source_class = source_primitive.__class__.__name__
            sink_class = ""
            sink_primitive = processor_ref.__meta__.get("sink")
            if sink_primitive:
                sink_class = sink_primitive.__class__.__name__

            source_child_resources = []
            sink_child_resources = []
            # Look for a resource for this processor_id
            processor_resource = find_processor_resource(processor_id)
            if processor_resource is not None:
                tracked_resources.add(processor_resource.resource_urn)

                source_urn = processor_resource.resource_outputs.get("source_urn", "")
                source_resource = resource_dict.get(source_urn)

                if source_resource:
                    source_child_resources = list(
                        find_attached_resources(source_resource).values()
                    )
                    tracked_resources.add(source_urn)
                    tracked_resources.update(
                        [res.resource_urn for res in source_child_resources]
                    )
                    num_pulumi_resources += len(source_child_resources)

                sink_urn = processor_resource.resource_outputs.get("sink_urn", "")
                sink_resource = resource_dict.get(sink_urn)

                if sink_resource:
                    sink_child_resources = list(
                        find_attached_resources(sink_resource).values()
                    )
                    tracked_resources.add(sink_urn)
                    tracked_resources.update(
                        [res.resource_urn for res in sink_child_resources]
                    )
                    num_pulumi_resources += len(sink_child_resources)

            source = None
            if source_class:
                source = PrimitiveState(
                    primitive_class=source_class,
                    resources=source_child_resources,
                )

            sink = None
            if sink_class:
                sink = PrimitiveState(
                    primitive_class=sink_class,
                    resources=sink_child_resources,
                )

            processor = ProcessorState(
                processor_id=processor_id,
                processor_type=processor_type,
                source=source,
                sink=sink,
            )
            processors.append(processor)

        untracked_resources = [
            res for res in resource_states if res.resource_urn not in tracked_resources
        ]
        # filter out any resources that are pulumi:providers or pulumi:stack
        untracked_resources = [
            res
            for res in untracked_resources
            if not res.resource_type.startswith("pulumi:")
        ]
        num_pulumi_resources += len(untracked_resources)

        return cls(
            flow_id=flow_id,
            processors=processors,
            untracked_resources=untracked_resources,
            last_updated=last_updated,
            num_pulumi_resources=num_pulumi_resources,
            pulumi_stack_name=pulumi_stack_name,
        )

    def as_json_dict(self):
        return {
            "flow_id": self.flow_id,
            "last_updated": self.last_updated.isoformat(),
            "num_pulumi_resources": self.num_pulumi_resources,
            "pulumi_stack_name": self.pulumi_stack_name,
            "processors": {p.processor_id: p.as_json_dict() for p in self.processors},
            "untracked_resources": [r.as_json_dict() for r in self.untracked_resources],
        }


def _lifecycle_functions(
    original_process_fn_or_class: Callable,
) -> Tuple[Callable, Callable, type_inspect.FullArgSpec]:
    """Returns the setup method, teardown method, and full arg spec respectfully."""
    if type_inspect.isclass(original_process_fn_or_class):

        def setup(self):
            if hasattr(self.instance, "setup"):
                self.instance.setup()

        async def teardown(self):
            coros = []
            if hasattr(self.instance, "teardown"):
                if type_inspect.iscoroutinefunction(self.instance.teardown()):
                    coros.append(self.instance.teardown())
                else:
                    self.instance.teardown()
            coros.extend([self.source().teardown(), self.sink().teardown()])
            await asyncio.gather(*coros)

    else:

        def setup(self):
            return None

        async def teardown(self):
            await asyncio.gather(self.source().teardown(), self.sink().teardown())

    return setup, teardown


def _background_tasks(
    primitive: Primitive, credentials: CredentialType
) -> List[BackgroundTask]:
    provider = primitive.background_task_provider()
    if provider is not None:
        return provider.background_tasks(credentials)
    return []


# NOTE: We do this outside of the Flow class to avoid the flow class
# being serialized with the processor.
def _consumer_processor(
    consumer: Consumer,
    source_credentials: CredentialType,
    sink_credentials: CredentialType,
):
    setup, teardown = _lifecycle_functions(consumer.original_process_fn_or_class)
    processor_id = consumer.original_process_fn_or_class.__name__

    def background_tasks():
        return _background_tasks(
            consumer.source_primitive, source_credentials
        ) + _background_tasks(consumer.sink_primitive, sink_credentials)

    # Dynamically define a new class with the same structure as Processor
    class_name = f"ConsumerProcessor{utils.uuid(max_len=8)}"
    source_provider = consumer.source_primitive.source_provider()
    sink_provider = consumer.sink_primitive.sink_provider()
    adhoc_methods = {
        # ConsumerProcessor methods.
        # NOTE: We need to instantiate the source and sink strategies
        # in the class to avoid issues passing to ray workers.
        "source": lambda self: source_provider.source(source_credentials),
        "sink": lambda self: sink_provider.sink(sink_credentials),
        # ProcessorAPI methods. NOTE: process() is attached separately below
        "setup": setup,
        "teardown": teardown,
        "background_tasks": lambda self: background_tasks(),
        "__meta__": {
            "source": consumer.source_primitive,
            "sink": consumer.sink_primitive,
        },
        "__call__": consumer.original_process_fn_or_class,
    }
    if type_inspect.isclass(consumer.original_process_fn_or_class):

        def init_processor(self, processor_id):
            self.processor_id = processor_id
            self.instance = consumer.original_process_fn_or_class()

        adhoc_methods["__init__"] = init_processor
    AdHocConsumerProcessorClass = type(
        class_name,
        (ConsumerProcessor,),
        adhoc_methods,
    )
    if not type_inspect.isclass(consumer.original_process_fn_or_class):
        utils.attach_method_to_class(
            AdHocConsumerProcessorClass,
            "process",
            original_func=consumer.original_process_fn_or_class,
        )
    else:
        utils.attach_wrapped_method_to_class(
            AdHocConsumerProcessorClass,
            "process",
            original_func=consumer.original_process_fn_or_class.process,
        )

    return AdHocConsumerProcessorClass(processor_id=processor_id)


def _collector_processor(collector: Collector, sink_credentials: CredentialType):
    setup, teardown = _lifecycle_functions(collector.original_process_fn_or_class)
    processor_id = collector.original_process_fn_or_class.__name__

    def background_tasks():
        return _background_tasks(collector.sink_primitive, sink_credentials)

    # Dynamically define a new class with the same structure as Processor
    class_name = f"CollectorProcessor{utils.uuid(max_len=8)}"
    sink_provider = collector.sink_primitive.sink_provider()
    adhoc_methods = {
        # CollectorProcessor methods.
        "endpoint": lambda self: RouteInfo(collector.route, collector.method),
        # NOTE: We need to instantiate the sink strategies
        # in the class to avoid issues passing to ray workers.
        "sink": lambda self: sink_provider.sink(sink_credentials),
        # ProcessorAPI methods. NOTE: process() is attached separately below
        "setup": setup,
        "teardown": teardown,
        "background_tasks": lambda self: background_tasks(),
        "__meta__": {
            "sink": collector.sink_primitive,
        },
        "__call__": collector.original_process_fn_or_class,
    }
    if type_inspect.isclass(collector.original_process_fn_or_class):

        def init_processor(self, processor_id):
            self.processor_id = processor_id
            self.instance = collector.original_process_fn_or_class()

        adhoc_methods["__init__"] = init_processor
    AdHocCollectorProcessorClass = type(
        class_name,
        (CollectorProcessor,),
        adhoc_methods,
    )
    if not type_inspect.isclass(collector.original_process_fn_or_class):
        utils.attach_method_to_class(
            AdHocCollectorProcessorClass,
            "process",
            original_func=collector.original_process_fn_or_class,
        )
    else:
        utils.attach_wrapped_method_to_class(
            AdHocCollectorProcessorClass,
            "process",
            original_func=collector.original_process_fn_or_class.process,
        )

    return AdHocCollectorProcessorClass(processor_id=processor_id)


def _endpoint_processor(endpoint: Endpoint, service_id: str):
    setup, teardown = _lifecycle_functions(endpoint.original_process_fn_or_class)

    processor_id = endpoint.original_process_fn_or_class.__name__

    # Dynamically define a new class with the same structure as Processor
    class_name = f"EndpointProcessor{utils.uuid(max_len=8)}"
    adhoc_methods = {
        # EndpointProcessor methods.
        "route_info": lambda self: RouteInfo(endpoint.route, endpoint.method),
        "service_id": lambda self: service_id,
        # NOTE: We need to instantiate the sink strategies
        # in the class to avoid issues passing to ray workers.
        # ProcessorAPI methods. NOTE: process() is attached separately below
        "setup": setup,
        "teardown": teardown,
        "background_tasks": lambda self: [],
        "__meta__": {},
        "__call__": endpoint.original_process_fn_or_class,
    }
    if type_inspect.isclass(endpoint.original_process_fn_or_class):

        def init_processor(self, processor_id):
            self.processor_id = processor_id
            self.instance = endpoint.original_process_fn_or_class()

        adhoc_methods["__init__"] = init_processor
    AdHocCollectorProcessorClass = type(
        class_name,
        (EndpointProcessor,),
        adhoc_methods,
    )
    if not type_inspect.isclass(endpoint.original_process_fn_or_class):
        utils.attach_method_to_class(
            AdHocCollectorProcessorClass,
            "process",
            original_func=endpoint.original_process_fn_or_class,
        )
    else:
        utils.attach_wrapped_method_to_class(
            AdHocCollectorProcessorClass,
            "process",
            original_func=endpoint.original_process_fn_or_class.process,
        )

    return AdHocCollectorProcessorClass(processor_id=processor_id)


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
        self._processor_groups: List[ProcessorGroup] = []
        # Runtime configuration
        self._runtime_actor_ref: Optional[RuntimeActor] = None
        # Infra configuration
        self._infra_actor_ref: Optional[InfraActor] = None
        self._managed_primitives: List[Primitive] = []
        self._services: List[Service] = []

    def _pulumi_program(self) -> List[pulumi.Resource]:
        visited_primitives = _PrimitiveCache()
        start_primitives = _find_primitives_with_no_parents(self._managed_primitives)
        if not start_primitives:
            raise ValueError(
                "Unable to build pulumi dependency tree. "
                "Is there a cycle in your pulumi dependencies?"
            )
        for primitive in start_primitives:
            creds = self._get_credentials(primitive.primitive_type)
            _traverse_primitive_for_pulumi(
                primitive=primitive,
                credentials=creds,
                initial_opts=pulumi.ResourceOptions(),
                visited_primitives=visited_primitives,
            )
        return [c.pulumi_resource for c in visited_primitives.cache]

    def manage(self, *args: Primitive):
        for primitive in args:
            primitive.enable_managed()
        self._managed_primitives.extend(args)

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

    # NOTE: The Flow class is responsible for converting Primitives into a Provider
    def consumer(
        self,
        source: Primitive,
        sink: Optional[Primitive] = None,
        *,
        num_cpus: float = 1.0,
        num_concurrency: int = 1,
        autoscale_options: AutoscalerOptions = AutoscalerOptions.default(),
        log_level: str = "INFO",
    ):
        if not dataclasses.is_dataclass(source):
            raise ValueError(
                f"source must be a dataclass. Received: {type(source).__name__}"
            )
        if sink is not None and not dataclasses.is_dataclass(sink):
            raise ValueError(
                f"sink must be a dataclass. Received: {type(sink).__name__}"
            )
        elif sink is None:
            sink = Empty()

        # Convert any Portableprimitives into cloud-specific primitives
        source = self._portable_primitive_to_cloud_primitive(source, StategyType.SOURCE)
        sink = self._portable_primitive_to_cloud_primitive(sink, StategyType.SINK)

        # Set up credentials
        source_credentials = self._get_credentials(source.primitive_type)
        sink_credentials = self._get_credentials(sink.primitive_type)

        return self._consumer_decorator(
            source_primitive=source,
            sink_primitive=sink,
            processor_options=ProcessorOptions(
                num_cpus=num_cpus,
                num_concurrency=num_concurrency,
                log_level=log_level,
                autoscaler_options=autoscale_options,
            ),
            source_credentials=source_credentials,
            sink_credentials=sink_credentials,
        )

    def add_consumer(self, consumer: Consumer):
        sink = self._portable_primitive_to_cloud_primitive(
            consumer.sink_primitive, StategyType.SINK
        )
        consumer.sink_primitive = sink
        source = self._portable_primitive_to_cloud_primitive(
            consumer.source_primitive, StategyType.SOURCE
        )
        consumer.source_primitive = source
        # Set up credentials
        source_credentials = self._get_credentials(
            consumer.source_primitive.primitive_type
        )
        sink_credentials = self._get_credentials(consumer.sink_primitive.primitive_type)
        processor = _consumer_processor(
            consumer=consumer,
            source_credentials=source_credentials,
            sink_credentials=sink_credentials,
        )
        group = ConsumerGroup(
            group_id=processor.processor_id,
            processors=[processor],
        )
        self._add_processor_group(group, consumer.processor_options)

    def add_collector(self, collector: Collector):
        sink = self._portable_primitive_to_cloud_primitive(
            collector.sink_primitive, StategyType.SINK
        )
        collector.sink_primitive = sink
        # Set up credentials
        sink_credentials = self._get_credentials(
            collector.sink_primitive.primitive_type
        )
        processor = _collector_processor(
            collector=collector,
            sink_credentials=sink_credentials,
        )
        group = CollectorGroup(
            group_id=processor.processor_id,
            processors=[processor],
        )
        self._add_processor_group(group, collector.processor_options)

    def collector(
        self,
        route: Route,
        method: Method,
        sink: Optional[Primitive] = None,
        *,
        num_cpus: float = 1.0,
        autoscale_options: AutoscalerOptions = AutoscalerOptions.default(),
        log_level: str = "INFO",
    ):
        if sink is None:
            sink = Empty()

        # Convert any Portableprimitives into cloud-specific primitives
        sink = self._portable_primitive_to_cloud_primitive(sink, StategyType.SINK)

        # Set up credentials
        sink_credentials = self._get_credentials(sink.primitive_type)

        return self._collector_decorator(
            route=route,
            method=method,
            sink_primitive=sink,
            processor_options=ProcessorOptions(
                num_cpus=num_cpus,
                # Collectors always have a concurrency of 1
                num_concurrency=1,
                log_level=log_level,
                autoscaler_options=autoscale_options,
            ),
            sink_credentials=sink_credentials,
        )

    def service(
        self,
        base_route: str = "/",
        *,
        num_cpus: float = 1.0,
        autoscale_options: AutoscalerOptions = AutoscalerOptions.default(),
        log_level: str = "INFO",
    ):
        # TODO: add pass in options
        service = Service(
            base_route=base_route,
            num_cpus=num_cpus,
            autoscale_options=autoscale_options,
            log_level=log_level,
        )
        self._services.append(service)
        return service

    def add_service(self, service: Service):
        self._services.append(service)

    def _add_service_groups(self):
        for service in self._services:
            endpoint_processors = []
            for endpoint in service.endpoints:
                processor = _endpoint_processor(
                    endpoint=endpoint, service_id=service.service_id
                )
                endpoint_processors.append(processor)
            self._add_processor_group(
                EndpointGroup(
                    base_route=service.base_route,
                    group_id=service.service_id,
                    processors=endpoint_processors,
                ),
                options=ProcessorOptions(
                    num_cpus=service.num_cpus,
                    num_concurrency=1,
                    log_level=service.log_level,
                    autoscaler_options=service.autoscale_options,
                ),
            )

    def _add_processor_group(self, group: ProcessorGroup, options: ProcessorOptions):
        if self._runtime_actor_ref is not None or self._infra_actor_ref is not None:
            raise RuntimeError(
                "Cannot add processor to an active Flow. Did you already call run()?"
            )
        if group.group_id in self.options.runtime_options.processor_options:
            raise RuntimeError(
                f"ProcessorGroup({group.group_id}) already exists in Flow object."
                "Please rename your function or remove the other Processor."
            )
        self.options.runtime_options.processor_options[group.group_id] = options
        self._processor_groups.append(group)

    def run(
        self,
        *,
        # runtime-only options
        debug_run: bool = False,
        run_id: Optional[RunID] = None,
        # server-only options. TODO: Move this into RuntimeOptions / consider
        # having Runtime manage the server
        start_runtime_server: bool = False,
        runtime_server_host: str = "127.0.0.1",
        runtime_server_port: int = 9653,
        # Options for testing
        block: bool = True,
    ):
        # Setup services
        self._add_service_groups()
        # Start the Flow Runtime
        runtime_coroutine = self._run(debug_run=debug_run)

        # Start the Runtime Server (maybe)
        if start_runtime_server:
            runtime_server = RuntimeServer(
                runtime_actor=self._get_runtime_actor(run_id=run_id),
                host=runtime_server_host,
                port=runtime_server_port,
            )
            with runtime_server.run_in_thread():
                server_log_message = (
                    "-" * 80
                    + "\n\n"
                    + f"Runtime Server running at http://{runtime_server_host}:{runtime_server_port}\n\n"
                    + "-" * 80
                    + "\n\n"
                )
                logging.info(server_log_message)
                print(server_log_message)
                if block:
                    asyncio.get_event_loop().run_until_complete(runtime_coroutine)
                else:
                    raise ValueError(
                        "Starting the Runtime Server is only "
                        "supported if blocking=True."
                    )
        else:
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
        await self._get_runtime_actor().run.remote(
            processor_groups=self._processor_groups
        )
        await self._get_runtime_actor().run_until_complete.remote()

    async def _drain(self):
        logging.debug(f"Draining Flow({self.flow_id})...")
        await self._get_runtime_actor().drain.remote()
        logging.debug(f"...Finished draining Flow({self.flow_id})")
        return True

    def refresh(self):
        return asyncio.get_event_loop().run_until_complete(self._refresh())

    async def _refresh(self):
        logging.debug(f"Refreshing Infra for Flow({self.flow_id})...")
        await self._get_infra_actor().refresh(pulumi_program=self._pulumi_program)
        logging.debug(f"...Finished refreshing Infra for Flow({self.flow_id})")

    def plan(self):
        return asyncio.get_event_loop().run_until_complete(self._plan())

    async def _plan(self):
        logging.debug(f"Planning Infra for Flow({self.flow_id})...")
        await self._get_infra_actor().plan(pulumi_program=self._pulumi_program)
        logging.debug(f"...Finished planning Infra for Flow({self.flow_id})")

    def apply(self):
        return asyncio.get_event_loop().run_until_complete(self._apply())

    async def _apply(self):
        logging.debug(f"Setting up Infra for Flow({self.flow_id})...")
        if self.options.infra_options.require_confirmation:
            await self._get_infra_actor().plan(pulumi_program=self._pulumi_program)
            print("Would you like to apply these changes?")
            response = input('Enter "y (yes)" to confirm, "n (no) to reject": ')
            while True:
                if response.lower() in ["n", "no"]:
                    print("User rejected Infra changes. Aborting.")
                    return
                elif response.lower() in ["y", "yes"]:
                    print("User confirmed Infra changes. Applying.")
                    break
                else:
                    response = input(
                        'Invalid response. Enter "y (yes)" to '
                        'confirm, "n (no) to reject": '
                    )
        await self._get_infra_actor().apply(pulumi_program=self._pulumi_program)
        logging.debug(f"...Finished setting up Infra for Flow({self.flow_id})")

    def destroy(self):
        return asyncio.get_event_loop().run_until_complete(self._destroy())

    async def _destroy(self):
        logging.debug(f"Tearing down infrastructure for Flow({self.flow_id})...")
        await self._get_infra_actor().destroy(pulumi_program=self._pulumi_program)
        logging.debug(
            f"...Finished tearing down infrastructure for Flow({self.flow_id})"
        )

    def inspect(self):
        # Setup services
        self._add_service_groups()
        pulumi_workspace = PulumiWorkspace(
            pulumi_options=self.options.infra_options.pulumi_options,
            pulumi_config=self.config.pulumi_config,
        )
        pulumi_stack_state = pulumi_workspace.get_stack_state()
        # TODO: update this to work with processor groups
        return FlowState.parse_resource_states(
            flow_id=self.flow_id,
            processor_refs=[],
            resource_states=pulumi_stack_state.resources(),
            last_updated=pulumi_stack_state.last_updated,
            pulumi_stack_name=pulumi_stack_state.stack_name,
        )

    def _lifecycle_functions(
        self, original_process_fn_or_class: Callable
    ) -> Tuple[Callable, Callable, type_inspect.FullArgSpec]:
        """Returns the setup method, teardown method, and full arg spec respectfully."""
        if type_inspect.isclass(original_process_fn_or_class):

            def setup(self):
                if hasattr(self.instance, "setup"):
                    self.instance.setup()

            async def teardown(self):
                coros = []
                if hasattr(self.instance, "teardown"):
                    if type_inspect.iscoroutinefunction(self.instance.teardown()):
                        coros.append(self.instance.teardown())
                    else:
                        self.instance.teardown()
                coros.extend([self.source().teardown(), self.sink().teardown()])
                await asyncio.gather(*coros)

        else:

            def setup(self):
                return None

            async def teardown(self):
                await asyncio.gather(self.source().teardown(), self.sink().teardown())

        return setup, teardown

    def _consumer_decorator(
        self,
        source_primitive: Primitive,
        sink_primitive: Primitive,
        processor_options: ProcessorOptions,
        source_credentials: CredentialType,
        sink_credentials: CredentialType,
    ):
        def decorator_function(original_process_fn_or_class):
            consumer = Consumer(
                source_primitive=source_primitive,
                sink_primitive=sink_primitive,
                processor_options=processor_options,
                original_process_fn_or_class=original_process_fn_or_class,
            )
            processor = _consumer_processor(
                consumer=consumer,
                source_credentials=source_credentials,
                sink_credentials=sink_credentials,
            )
            group = ConsumerGroup(
                group_id=processor.processor_id,
                processors=[processor],
            )
            self._add_processor_group(group, consumer.processor_options)
            return processor

        return decorator_function

    def _collector_decorator(
        self,
        route: Route,
        method: Method,
        sink_primitive: Primitive,
        processor_options: ProcessorOptions,
        sink_credentials: CredentialType,
    ):
        def decorator_function(original_process_fn_or_class):
            collector = Collector(
                # TODO: whenever we allow collector groups we'll need to update this
                route="/",
                method=method,
                sink_primitive=sink_primitive,
                processor_options=processor_options,
                original_process_fn_or_class=original_process_fn_or_class,
            )
            processor = _collector_processor(
                collector=collector, sink_credentials=sink_credentials
            )
            group = CollectorGroup(
                group_id=processor.processor_id,
                processors=[processor],
                base_route=route,
            )
            self._add_processor_group(group, collector.processor_options)
            return processor

        return decorator_function
