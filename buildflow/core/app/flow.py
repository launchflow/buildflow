import asyncio
import dataclasses
import inspect as type_inspect
import logging
import os
import signal
import sys
from typing import Any, Callable, Dict, List, Optional, Tuple

import pulumi
import ray
from rich.progress import Progress
from rich.prompt import Prompt

import buildflow
from buildflow.config.buildflow_config import BuildFlowConfig
from buildflow.core import utils
from buildflow.core.app.collector import Collector
from buildflow.core.app.consumer import Consumer
from buildflow.core.app.endpoint import Endpoint
from buildflow.core.app.flow_state import (
    CollectorGroupState,
    CollectorState,
    ConsumerGroupState,
    ConsumerState,
    EndpointState,
    FlowState,
    PrimitiveState,
    ProcessorGroupState,
    ProcessorState,
    ServiceState,
)
from buildflow.core.app.infra.actors.infra import InfraActor
from buildflow.core.app.runtime._runtime import RunID
from buildflow.core.app.runtime.actors.runtime import RuntimeActor
from buildflow.core.app.runtime.server import RuntimeServer
from buildflow.core.app.service import Service
from buildflow.core.background_tasks.background_task import BackgroundTask
from buildflow.core.credentials._credentials import CredentialType
from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.core.credentials.empty_credentials import EmptyCredentials
from buildflow.core.credentials.gcp_credentials import GCPCredentials
from buildflow.core.infra.buildflow_resource import BuildFlowResource
from buildflow.core.options.flow_options import FlowOptions
from buildflow.core.options.runtime_options import AutoscalerOptions, ProcessorOptions
from buildflow.core.processor.patterns.collector import (
    CollectorGroup,
    CollectorProcessor,
)
from buildflow.core.processor.patterns.consumer import ConsumerGroup, ConsumerProcessor
from buildflow.core.processor.patterns.endpoint import EndpointGroup, EndpointProcessor
from buildflow.core.processor.processor import (
    ProcessorGroup,
    ProcessorGroupType,
    ProcessorType,
)
from buildflow.dependencies.base import DependencyWrapper, dependency_wrappers
from buildflow.dependencies.flow_dependencies import FlowCredentials
from buildflow.exceptions.exceptions import PathNotFoundException
from buildflow.io.endpoint import Method, Route, RouteInfo
from buildflow.io.local.empty import Empty
from buildflow.io.primitive import (
    PortablePrimtive,
    Primitive,
    PrimitiveDependency,
    PrimitiveType,
)
from buildflow.io.strategies._strategy import StategyType


@dataclasses.dataclass
class _PrimitiveCacheEntry:
    primitive: Primitive
    buildflow_resource: BuildFlowResource


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
                return entry.buildflow_resource
        return None

    def clear(self):
        self.cache.clear()


def _find_managed_primitives(field_value: Any) -> List[Primitive]:
    primitives = []
    if field_value is None:
        return []
    elif isinstance(field_value, Primitive) and field_value._managed:
        primitives.append(field_value)
    elif isinstance(field_value, list):
        for item in field_value:
            primitives.extend(_find_managed_primitives(item))
    return primitives


def _find_primitives_with_no_parents(primitives: List[Primitive]) -> List[Primitive]:
    primitives_without_parents = primitives.copy()
    for primitive in primitives:
        fields = dataclasses.fields(primitive)
        for field in fields:
            field_value = getattr(primitive, field.name)
            managed_primitives = _find_managed_primitives(field_value)
            for managed_primitive in managed_primitives:
                try:
                    primitives_without_parents.remove(managed_primitive)
                except ValueError:
                    # This can happen when a primitive is a parent of two or more
                    # other primitives.
                    pass
    return primitives_without_parents


def _find_all_managed_parent_primitives(primitive: Primitive) -> Dict[str, Primitive]:
    fields = dataclasses.fields(primitive)
    managed_parents = {}
    for field in fields:
        field_value = getattr(primitive, field.name)
        managed_primitives = _find_managed_primitives(field_value)
        for managed_primitive in managed_primitives:
            managed_parents[managed_primitive.primitive_id()] = managed_primitive
            managed_parents.update(
                _find_all_managed_parent_primitives(managed_primitive)
            )
    return managed_parents


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
    fields = dataclasses.fields(primitive)
    parent_resources = []
    for field in fields:
        field_value = getattr(primitive, field.name)
        managed_primitives = _find_managed_primitives(field_value)
        for managed_primitive in managed_primitives:
            visited_resource = visited_primitives.get(managed_primitive)
            if visited_resource is None:
                parent_resources.append(
                    _traverse_primitive_for_pulumi(
                        managed_primitive,
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

    resource = BuildFlowResource(primitive, credentials, opts)
    visited_primitives.append(_PrimitiveCacheEntry(primitive, resource))
    return resource


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
    return primitive.background_tasks(credentials)


# NOTE: We do this outside of the Flow class to avoid the flow class
# being serialized with the processor.
def _consumer_processor(
    consumer: Consumer,
    source_credentials: CredentialType,
    sink_credentials: CredentialType,
):
    setup, teardown = _lifecycle_functions(consumer.original_process_fn_or_class)
    processor_id = consumer.original_process_fn_or_class.__name__
    dependencies, _ = dependency_wrappers(consumer.original_process_fn_or_class)

    def background_tasks():
        return _background_tasks(
            consumer.source_primitive, source_credentials
        ) + _background_tasks(consumer.sink_primitive, sink_credentials)

    # Dynamically define a new class with the same structure as Processor
    class_name = f"ConsumerProcessor{utils.uuid(max_len=8)}"
    adhoc_methods = {
        # ConsumerProcessor methods.
        # NOTE: We need to instantiate the source and sink strategies
        # in the class to avoid issues passing to ray workers.
        "source": lambda self: consumer.source_primitive.source(source_credentials),
        "sink": lambda self: consumer.sink_primitive.sink(sink_credentials),
        # ProcessorAPI methods. NOTE: process() is attached separately below
        "setup": setup,
        "teardown": teardown,
        "background_tasks": lambda self: background_tasks(),
        "dependencies": lambda self: dependencies,
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
    dependencies, _ = dependency_wrappers(collector.original_process_fn_or_class)

    def background_tasks():
        return _background_tasks(collector.sink_primitive, sink_credentials)

    # Dynamically define a new class with the same structure as Processor
    class_name = f"CollectorProcessor{utils.uuid(max_len=8)}"
    adhoc_methods = {
        # CollectorProcessor methods.
        "route_info": lambda self: RouteInfo(collector.route, collector.method),
        # NOTE: We need to instantiate the sink strategies
        # in the class to avoid issues passing to ray workers.
        "sink": lambda self: collector.sink_primitive.sink(sink_credentials),
        # ProcessorAPI methods. NOTE: process() is attached separately below
        "setup": setup,
        "teardown": teardown,
        "background_tasks": lambda self: background_tasks(),
        "dependencies": lambda self: dependencies,
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
    dependencies, _ = dependency_wrappers(endpoint.original_process_fn_or_class)

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
        "dependencies": lambda self: dependencies,
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


def _find_primitive_deps(deps: List[DependencyWrapper]) -> List[Primitive]:
    primitive_deps = []
    for dep in deps:
        if isinstance(dep.dependency, PrimitiveDependency):
            primitive_deps.append(dep.dependency.primitive)
        for sub_dep in dep.dependency.sub_dependencies:
            primitive_deps.extend(_find_primitive_deps([sub_dep]))
    return primitive_deps


class Flow:
    def __init__(
        self,
        flow_id: FlowID = "buildflow-app",
        flow_options: Optional[FlowOptions] = None,
    ) -> None:
        try:
            self.config = BuildFlowConfig.load(_get_directory_path_of_caller())
        except PathNotFoundException:
            self.config = None

        # Flow configuration
        self.flow_id = flow_id
        self.options = flow_options or FlowOptions.default()
        # Flow initial state
        self._processor_groups: List[ProcessorGroup] = []
        # Runtime configuration
        self._runtime_actor_ref: Optional[RuntimeActor] = None
        # Infra configuration
        self._infra_actor_ref: Optional[InfraActor] = None
        self._managed_primitives: Dict[str, Primitive] = {}
        self._services: List[Service] = []
        self.credentials = FlowCredentials(
            gcp_credentials=GCPCredentials(self.options.credentials_options),
            aws_credentials=AWSCredentials(self.options.credentials_options),
        )
        self.flow_dependencies = {FlowCredentials: self.credentials}

    def _pulumi_program(self) -> List[pulumi.Resource]:
        visited_primitives = _PrimitiveCache()
        start_primitives = _find_primitives_with_no_parents(
            list(self._managed_primitives.values())
        )
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
        return [c.buildflow_resource for c in visited_primitives.cache]

    def manage(self, *args: Primitive):
        for primitive in args:
            if not isinstance(primitive, Primitive):
                raise ValueError(
                    f"manage must be called with a Primitive type. Got: {primitive}"
                )
            primitive.enable_managed()
            self._managed_primitives[primitive.primitive_id()] = primitive

            self._managed_primitives.update(
                _find_all_managed_parent_primitives(primitive)
            )

    def _get_infra_actor(self) -> InfraActor:
        if self.config is None:
            raise RuntimeError(
                "Unable to create InfraActor. "
                "No BuildFlow config found. Did you run `buildflow init`?"
            )
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
                flow_dependencies=self.flow_dependencies,
            )
        return self._runtime_actor_ref

    def _get_credentials(self, primitive_type: PrimitiveType):
        if primitive_type == PrimitiveType.GCP:
            return self.credentials.gcp_credentials
        elif primitive_type == PrimitiveType.AWS:
            return self.credentials.aws_credentials
        return EmptyCredentials()

    def _portable_primitive_to_cloud_primitive(
        self, primitive: Primitive, strategy_type: StategyType
    ):
        if primitive.primitive_type == PrimitiveType.PORTABLE:
            if self.config is None:
                raise RuntimeError(
                    "Unable to create portable primitive. "
                    "No BuildFlow config found. Did you run `buildflow init`?"
                )
            primitive: PortablePrimtive
            primitive = primitive.to_cloud_primitive(
                cloud_provider_config=self.config.cloud_provider_config,
                strategy_type=strategy_type,
            )
            primitive.enable_managed()
        return primitive

    # NOTE: The Flow class is responsible for converting Primitives into a strategy
    def consumer(
        self,
        source: Primitive,
        sink: Optional[Primitive] = None,
        *,
        num_cpus: float = 1.0,
        num_concurrency: int = 1,
        enable_autoscaler: bool = True,
        num_replicas: int = 1,
        min_replicas: int = 1,
        max_replicas: int = 1000,
        autoscale_frequency_secs: int = 60,
        consumer_backlog_burn_threshold: int = 60,
        consumer_cpu_percent_target: int = 25,
        log_level: str = "INFO",
    ):
        autoscale_options = AutoscalerOptions(
            enable_autoscaler=enable_autoscaler,
            num_replicas=num_replicas,
            min_replicas=min_replicas,
            max_replicas=max_replicas,
            autoscale_frequency_secs=autoscale_frequency_secs,
            consumer_backlog_burn_threshold=consumer_backlog_burn_threshold,
            consumer_cpu_percent_target=consumer_cpu_percent_target,
        )
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
        if consumer.sink_primitive is not None and not dataclasses.is_dataclass(
            consumer.sink_primitive
        ):
            raise ValueError(
                f"sink must be a dataclass. Received: {type(consumer.sink_primitive).__name__}"  # noqa
            )
        elif consumer.sink_primitive is None:
            consumer.sink_primitive = Empty()
        consumer.sink_primitive = self._portable_primitive_to_cloud_primitive(
            consumer.sink_primitive, StategyType.SINK
        )
        consumer.source_primitive = self._portable_primitive_to_cloud_primitive(
            consumer.source_primitive, StategyType.SOURCE
        )
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
        if collector.sink_primitive is not None and not dataclasses.is_dataclass(
            collector.sink_primitive
        ):
            raise ValueError(
                f"sink must be a dataclass. Received: {type(collector.sink_primitive).__name__}"  # noqa
            )
        elif collector.sink_primitive is None:
            collector.sink_primitive = Empty()
        collector.sink_primitive = self._portable_primitive_to_cloud_primitive(
            collector.sink_primitive, StategyType.SINK
        )
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
        max_concurrent_queries: int = 100,
        enable_autoscaler: bool = True,
        num_replicas: int = 1,
        min_replicas: int = 1,
        max_replicas: int = 1000,
        target_num_ongoing_requests_per_replica: int = 1,
        log_level: str = "INFO",
    ):
        if isinstance(method, str):
            method = Method(method.upper())
        if method == Method.WEBSOCKET:
            raise NotImplementedError("Websocket collectors are not yet supported")

        autoscale_options = AutoscalerOptions(
            enable_autoscaler=enable_autoscaler,
            num_replicas=num_replicas,
            min_replicas=min_replicas,
            max_replicas=max_replicas,
            target_num_ongoing_requests_per_replica=target_num_ongoing_requests_per_replica,
            max_concurrent_queries=max_concurrent_queries,
        )
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
        service_id: str = utils.uuid(),
        num_cpus: float = 1.0,
        enable_autoscaler: bool = True,
        num_replicas: int = 1,
        min_replicas: int = 1,
        max_replicas: int = 1000,
        max_concurrent_queries: int = 100,
        target_num_ongoing_requests_per_replica: int = 1,
        log_level: str = "INFO",
    ):
        service = Service(
            base_route=base_route,
            num_cpus=num_cpus,
            enable_autoscaler=enable_autoscaler,
            max_concurrent_queries=max_concurrent_queries,
            num_replicas=num_replicas,
            min_replics=min_replicas,
            max_replicas=max_replicas,
            target_num_ongoing_requests_per_replica=target_num_ongoing_requests_per_replica,
            log_level=log_level,
            service_id=service_id,
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
                    middleware=service.middleware,
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
        flow_state: Optional[FlowState] = None,
        # runtime-only options
        debug_run: bool = False,
        run_id: Optional[RunID] = None,
        # global ray serve options consumed by collectors and endpoints
        serve_host: str = "127.0.0.1",
        serve_port: int = 8000,
        # runtime server-only options.
        # having Runtime manage the server
        start_runtime_server: bool = False,
        runtime_server_host: str = "127.0.0.1",
        runtime_server_port: int = 9653,
        # Options for testing
        block: bool = True,
        event_subscriber: Optional[Callable] = None,
    ):
        self._add_service_groups()
        if not self._processor_groups:
            logging.warning("Flow contains no processors. Exiting.")
            return
        if flow_state is None:
            flow_state = self._flowstate()
        try:
            # There's an issue on ray where if we don't do this ray will
            # start two clusters on mac
            ray.init(address="auto", ignore_reinit_error=True)
        except ConnectionError:
            ray.init(ignore_reinit_error=True)
        # Setup services
        # Start the Flow Runtime
        runtime_coroutine = self._run(
            debug_run=debug_run,
            serve_host=serve_host,
            serve_port=serve_port,
            event_subscriber=event_subscriber,
        )

        if debug_run:
            # If debug run is enabled, we want to set the checkin frequency
            # more often so we can reload faster
            self.options.runtime_options.checkin_frequency_loop_secs = 1

        # Start the Runtime Server (maybe)
        if start_runtime_server:
            runtime_server = RuntimeServer(
                runtime_actor=self._get_runtime_actor(run_id=run_id),
                host=runtime_server_host,
                port=runtime_server_port,
                flow_state=flow_state,
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

    async def _run(
        self,
        serve_host: str,
        serve_port: int,
        debug_run: bool = False,
        event_subscriber: Optional[Callable] = None,
    ):
        # Add a signal handler to drain the runtime when the process is killed
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig,
                lambda: asyncio.create_task(self._drain(as_reload=debug_run)),
            )
        # start the runtime and await it to finish
        # NOTE: run() does not necessarily block until the runtime is finished.
        await self._get_runtime_actor().run.remote(
            processor_groups=self._processor_groups,
            serve_host=serve_host,
            serve_port=serve_port,
            event_subscriber=event_subscriber,
        )
        await self._get_runtime_actor().run_until_complete.remote()

    async def _drain(self, as_reload: bool = False):
        logging.debug(f"Draining Flow({self.flow_id})...")
        await self._get_runtime_actor().drain.remote(as_reload=as_reload)
        logging.debug(f"...Finished draining Flow({self.flow_id})")
        return True

    def refresh(self):
        return asyncio.get_event_loop().run_until_complete(self._refresh())

    async def _refresh(self):
        logging.debug(f"Refreshing Infra for Flow({self.flow_id})...")
        await self._get_infra_actor().refresh(pulumi_program=self._pulumi_program)
        logging.debug(f"...Finished refreshing Infra for Flow({self.flow_id})")

    def preview(self, progress: Optional[Progress] = None):
        return asyncio.get_event_loop().run_until_complete(self._preview(progress))

    async def _preview(self, progress: Optional[Progress] = None):
        logging.debug(f"Previewing Infra for Flow({self.flow_id})...")
        if progress is not None:
            preview_task = progress.add_task("Generating Preview...", total=1)
        await self._get_infra_actor().preview(
            pulumi_program=self._pulumi_program,
            managed_primitives=self._managed_primitives,
        )
        if progress is not None:
            progress.advance(preview_task)
            progress.remove_task(preview_task)
        logging.debug(f"...Finished previewing Infra for Flow({self.flow_id})")

    def apply(self, progress: Optional[Progress] = None):
        return asyncio.get_event_loop().run_until_complete(self._apply(progress))

    async def _apply(self, progress: Optional[Progress] = None):
        logging.debug(f"Setting up Infra for Flow({self.flow_id})...")
        if self.options.infra_options.require_confirmation:
            await self._preview(progress)
            print("Would you like to apply these changes?")
            if progress is not None:
                progress.stop()
            response = Prompt.ask(
                'Enter "y (yes)" to confirm, "n (no) to reject": ',
                choices=["y", "n", "yes", "no"],
            )
            if response.lower() in ["n", "no"]:
                if progress is not None:
                    progress.console.print("[red]✗[/red] Changes rejected.")
                else:
                    print("User rejected Infra changes. Aborting.")
                return
        else:
            print("Confirmation disabled.")
        if progress is not None:
            progress.start()
            apply_task = progress.add_task("Applying changes...", total=1)
        await self._get_infra_actor().apply(pulumi_program=self._pulumi_program)
        if progress is not None:
            progress.advance(apply_task)
            progress.remove_task(apply_task)
            progress.console.print("[green]✓[/green] Changes applied.")
        else:
            print("Changes applied.")

    def destroy(self, progress: Optional[Progress] = None):
        return asyncio.get_event_loop().run_until_complete(self._destroy(progress))

    async def _destroy(self, progress: Optional[Progress] = None):
        logging.debug(f"Destroying infrastructure for Flow({self.flow_id})...")
        if self.options.infra_options.require_confirmation:
            # We pass in no managed primitives, to simulate deleting all infrastructure.
            if progress is not None:
                preview_task = progress.add_task("Generating Preview...", total=1)
            await self._get_infra_actor().preview(
                pulumi_program=self._pulumi_program, managed_primitives={}
            )
            if progress is not None:
                progress.advance(preview_task)
                progress.remove_task(preview_task)
            print("Would you like to apply these changes?")
            if progress is not None:
                progress.stop()
            response = Prompt.ask(
                'Enter "y (yes)" to confirm, "n (no) to reject": ',
                choices=["y", "n", "yes", "no"],
            )
            if response.lower() in ["n", "no"]:
                if progress is not None:
                    progress.console.print("[red]✗[/red] Changes rejected.")
                else:
                    print("User rejected Infra changes. Aborting.")
                return
        else:
            print("Confirmation disabled.")
        if progress is not None:
            progress.start()
            apply_task = progress.add_task("Destroying resources...", total=1)
        await self._get_infra_actor().destroy(pulumi_program=self._pulumi_program)
        if progress is not None:
            progress.advance(apply_task)
            progress.remove_task(apply_task)
            progress.console.print("[green]✓[/green] Resources destroyed.")
        else:
            print("Resources destroyed.")

    def flowstate(self) -> FlowState:
        self._add_service_groups()
        return self._flowstate()

    def _flowstate(self) -> FlowState:
        primitive_states = {}
        managed_primitives = set()
        for managed_resource in self._managed_primitives.values():
            managed_primitives.add(managed_resource.primitive_id())
            states = PrimitiveState.from_primitive(
                managed_resource,
                managed_primitives=managed_primitives,
                visited_primitives=set(),
            )
            primitive_states.update({ps.primitive_id: ps for ps in states})

        processor_group_states = []
        for group in self._processor_groups:
            processor_states = []
            for processor in group.processors:
                if processor.processor_type == ProcessorType.CONSUMER:
                    sink_id = None
                    if "sink" in processor.__meta__:
                        sink = processor.__meta__["sink"]
                        sink_id = sink.primitive_id()
                        if (
                            not isinstance(sink, Empty)
                            and sink_id not in primitive_states
                        ):
                            states = PrimitiveState.from_primitive(
                                sink,
                                managed_primitives=managed_primitives,
                                visited_primitives=set(primitive_states.keys()),
                            )
                            primitive_states.update(
                                {ps.primitive_id: ps for ps in states}
                            )
                    source = processor.__meta__["source"]
                    if source.primitive_id() not in primitive_states:
                        states = PrimitiveState.from_primitive(
                            source,
                            managed_primitives=managed_primitives,
                            visited_primitives=set(primitive_states.keys()),
                        )
                        primitive_states.update({ps.primitive_id: ps for ps in states})
                    processor_info = ConsumerState(
                        source_id=source.primitive_id(),
                        sink_id=sink_id,
                    )
                elif processor.processor_type == ProcessorType.COLLECTOR:
                    sink_id = None
                    if "sink" in processor.__meta__:
                        sink = processor.__meta__["sink"]
                        sink_id = sink.primitive_id()
                        if (
                            not isinstance(sink, Empty)
                            and sink_id not in primitive_states
                        ):
                            states = PrimitiveState.from_primitive(
                                sink,
                                managed_primitives=managed_primitives,
                                visited_primitives=set(primitive_states.keys()),
                            )
                            primitive_states.update(
                                {ps.primitive_id: ps for ps in states}
                            )
                    processor_info = CollectorState(sink_id=sink_id)
                elif processor.processor_type == ProcessorType.ENDPOINT:
                    end_route = processor.route_info().route
                    if end_route.startswith(
                        group.base_route
                    ) and group.base_route.endswith("/"):
                        end_route = end_route[len(group.base_route) :]
                    route = group.base_route + end_route
                    processor_info = EndpointState(
                        route=route,
                        method=processor.route_info().method,
                    )
                dependencies = _find_primitive_deps(processor.dependencies())
                primitive_dependencies = []
                for prim in dependencies:
                    primitive_dependencies.append(prim.primitive_id())
                    if prim.primitive_id() not in primitive_states:
                        states = PrimitiveState.from_primitive(
                            prim,
                            managed_primitives=managed_primitives,
                            visited_primitives=set(primitive_states.keys()),
                        )
                        primitive_states.update({ps.primitive_id: ps for ps in states})

                processor_state = ProcessorState(
                    processor_id=processor.processor_id,
                    processor_type=processor.processor_type,
                    processor_info=processor_info,
                    primitive_dependencies=primitive_dependencies,
                )
                processor_states.append(processor_state)
            group_info = None
            if group.group_type == ProcessorGroupType.CONSUMER:
                group_info = ConsumerGroupState()
            elif group.group_type == ProcessorGroupType.COLLECTOR:
                group_info = CollectorGroupState(base_route=group.base_route)
            elif group.group_type == ProcessorGroupType.SERVICE:
                group_info = ServiceState(base_route=group.base_route)
            processor_group_states.append(
                ProcessorGroupState(
                    processor_group_id=group.group_id,
                    processor_states=processor_states,
                    processor_group_type=group.group_type,
                    group_info=group_info,
                )
            )
        if self.options.infra_options.pulumi_options.selected_stack is not None:
            stack = self.options.infra_options.pulumi_options.selected_stack
        else:
            stack = self.config.pulumi_config.default_stack
        return FlowState(
            flow_id=self.flow_id,
            stack=stack,
            primitive_states=list(primitive_states.values()),
            processor_group_states=processor_group_states,
            python_version=f"{sys.version_info.major}.{sys.version_info.minor}",
            ray_version=ray.__version__,
            buildflow_version=buildflow.__version__,
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
