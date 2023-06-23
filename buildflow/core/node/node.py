import asyncio
import inspect
import logging
import signal
from functools import wraps
from typing import List, Optional

from ray import serve
import ray

from buildflow import utils
from buildflow.api import (
    NodeAPI,
    NodeApplyResult,
    NodeDestroyResult,
    NodeID,
    NodePlan,
    SinkType,
    SourceType,
)
from buildflow.api.infra import InfraTag
from buildflow.core.infra.actors.infra import InfraActor
from buildflow.core.infra.config import InfraConfig
from buildflow.core.node.server import NodeServer
from buildflow.core.processor import Processor
from buildflow.core.runtime.actors.runtime import RuntimeActor
from buildflow.core.runtime.config import ReplicaConfig, RuntimeConfig
from buildflow.resources.io.registry import EmptySink
from buildflow.resources.config import ResourceConfig
from buildflow.resources import MetaResourceType
from buildflow.resources.io import ResourceType
import copy


def _attach_method(cls, func):
    if inspect.iscoroutinefunction(func):

        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            return await func(*args, **kwargs)

    else:

        @wraps(func)
        def wrapper(self, *args, **kwargs):
            return func(*args, **kwargs)

    sig = inspect.signature(func)
    params = [inspect.Parameter("self", inspect.Parameter.POSITIONAL_OR_KEYWORD)]
    params.extend(sig.parameters.values())
    wrapper.__signature__ = sig.replace(parameters=params)
    setattr(cls, "process", wrapper)


def _processor_decorator(
    node: "Node",
    source: SourceType,
    sink: Optional[SinkType] = None,
    *,
    num_cpus: float = 1.0,
    num_concurrency: int = 1,
    log_level: str = None,
):
    if sink is None:
        sink = EmptySink()

    def decorator_function(original_function):
        processor_id = original_function.__name__
        # Dynamically define a new class with the same structure as Processor
        class_name = f"AdHocProcessor_{utils.uuid(max_len=8)}"

        def wrapper_function(*args, **kwargs):
            return original_function(*args, **kwargs)

        _AdHocProcessor = type(
            class_name,
            (Processor,),
            {
                "source": lambda self: source,
                "sink": lambda self: sink,
                "sinks": lambda self: [],
                "setup": lambda self: None,
                "__call__": wrapper_function,
            },
        )
        _attach_method(_AdHocProcessor, original_function)
        processor_instance = _AdHocProcessor(processor_id=processor_id)
        node.add(
            processor_instance,
            num_cpus=num_cpus,
            num_concurrency=num_concurrency,
            log_level=log_level,
        )

        return processor_instance

    return decorator_function


# NOTE: Node implements NodeAPI, which is a combination of RuntimeAPI and InfraAPI.
class Node(NodeAPI):
    def __init__(
        self,
        node_id: NodeID = "buildflow-node",
        runtime_config: Optional[RuntimeConfig] = None,
        infra_config: Optional[InfraConfig] = None,
        resource_config: Optional[ResourceConfig] = None,
    ) -> None:
        self.node_id = node_id
        self._processors: List[Processor] = []
        # The Node class is a wrapper around the Runtime and Infrastructure
        self._runtime_config = runtime_config or RuntimeConfig.IO_BOUND()
        self._runtime_actor = RuntimeActor.remote(self._runtime_config)
        self._infra_config = infra_config or InfraConfig.DEFAULT()
        self._infra_actor = InfraActor.remote(self._infra_config, tag="default")
        self._resource_config = resource_config or ResourceConfig.DEFAULT()

    def processor(
        self,
        source: SourceType,
        sink: Optional[SinkType] = None,
        num_cpus: float = 1.0,
        num_concurrency: int = 1,
        log_level: str = None,
    ):
        # NOTE: processor_decorator is a function that returns an Ad Hoc
        # Processor implementation.
        return _processor_decorator(
            node=self,
            source=source,
            sink=sink,
            num_cpus=num_cpus,
            num_concurrency=num_concurrency,
            log_level=log_level,
        )

    def add(
        self,
        processor: Processor,
        *,
        num_cpus: float = 1.0,
        num_concurrency: int = 1,
        log_level: str = None,
    ):
        if ray.get(self._runtime_actor.is_active.remote()):
            raise RuntimeError("Cannot add processor to a node with an active runtime.")
        if ray.get(self._infra_actor.is_active.remote()):
            raise RuntimeError("Cannot add processor to a node with an active infra.")
        if processor.processor_id in self._runtime_config.replica_configs:
            raise RuntimeError(
                f"Processor({processor.processor_id}) already exists in node."
            )
        if log_level is None:
            log_level = self._runtime_config.log_level

        # Attach the get_resource_config and get_io_type methods to the processor's
        # source() method. This lets us inject the resource config and type at runtime.
        if isinstance(processor.source(), MetaResourceType):
            resource_config_copy = copy.copy(self._resource_config)
            setattr(
                processor.source(),
                "get_resource_config",
                lambda: resource_config_copy,
            )
            setattr(processor.source(), "get_io_type", lambda: SourceType)
        # Attach the get_resource_config and get_io_type methods to the processor's
        # sink() method. This lets us inject the resource config and type at runtime.
        if isinstance(processor.sink(), MetaResourceType):
            resource_config_copy = copy.copy(self._resource_config)
            setattr(
                processor.sink(),
                "get_resource_config",
                lambda: resource_config_copy,
            )
            setattr(processor.sink(), "get_io_type", lambda: SinkType)

        # Each processor gets its own replica config
        self._runtime_config.replica_configs[processor.processor_id] = ReplicaConfig(
            num_cpus=num_cpus,
            num_concurrency=num_concurrency,
            log_level=log_level,
        )
        self._processors.append(processor)

    def run(
        self,
        *,
        disable_usage_stats: bool = False,
        # runtime-only options
        block_runtime: bool = True,
        debug_run: bool = False,
        # infra-only options
        apply_infrastructure: bool = False,
        destroy_infrastructure: bool = False,
        # node server options
        start_node_server: bool = False,
        node_server_host: str = "127.0.0.1",
        node_server_port: int = 9653,
    ):
        coro = self._run_async(
            disable_usage_stats=disable_usage_stats,
            apply_infrastructure=apply_infrastructure,
            destroy_infrastructure=destroy_infrastructure,
            debug_run=debug_run,
            start_node_server=start_node_server,
            node_server_host=node_server_host,
            node_server_port=node_server_port,
        )
        if block_runtime:
            asyncio.get_event_loop().run_until_complete(coro)
        else:
            return coro

    async def _run_async(
        self,
        *,
        disable_usage_stats: bool,
        apply_infrastructure: bool,
        destroy_infrastructure: bool,
        debug_run: bool,
        start_node_server: bool,
        node_server_host: str,
        node_server_port: int,
    ):
        # BuildFlow Usage Stats
        if not disable_usage_stats:
            utils.log_buildflow_usage()

        # BuildFlow Node Server
        if start_node_server:
            node_server = NodeServer.bind(runtime_actor=self._runtime_actor)
            serve.run(
                node_server,
                host=node_server_host,
                port=node_server_port,
            )
            server_log_message = (
                "-" * 80
                + "\n\n"
                + f"Node Server running at http://{node_server_host}:{node_server_port}\n\n"
                + "-" * 80
                + "\n\n"
            )
            logging.info(server_log_message)
            print(server_log_message)

        # BuildFlow Resource Creation
        if apply_infrastructure:
            await self._apply_async()

        # BuildFlow Runtime
        # schedule cleanup
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig,
                lambda: asyncio.create_task(
                    self._drain(destroy=destroy_infrastructure)
                ),
            )
        # start the runtime
        await self._runtime_actor.run.remote(processors=self._processors)

        remote_runtime_task = self._runtime_actor.run_until_complete.remote()
        if destroy_infrastructure:
            runtime_future: asyncio.Future = asyncio.wrap_future(
                remote_runtime_task.future()
            )
            # schedule the destroy to run after the runtime is done.
            # NOTE: This will only run if the runtime finishes successfully.
            runtime_future.add_done_callback(lambda _: self._destroy_async())
        return await remote_runtime_task

    async def _drain(self, destroy: bool = False):
        logging.debug(f"Draining Node({self.node_id})...")
        await self._runtime_actor.drain.remote()
        logging.debug(f"...Finished draining Node({self.node_id})")
        if destroy:
            await self._destroy_async()
        return True

    def plan(self, tag: InfraTag = "default") -> NodePlan:
        return asyncio.get_event_loop().run_until_complete(self._plan_async(tag=tag))

    async def _plan_async(self, *, tag: InfraTag) -> NodePlan:
        logging.debug(f"Planning Infra(tag={tag}) for Node({self.node_id})...")
        result = await self._infra_actor.plan.remote(processors=self._processors)
        logging.debug(f"...Finished planning Infra(tag={tag}) for Node({self.node_id})")
        return result

    def apply(self, tag: InfraTag = "default") -> NodeApplyResult:
        return asyncio.get_event_loop().run_until_complete(self._apply_async(tag=tag))

    async def _apply_async(self, *, tag: InfraTag) -> NodeApplyResult:
        logging.debug(f"Setting up Infra(tag={tag}) for Node({self.node_id})...")
        result = await self._infra_actor.apply.remote(processors=self._processors)
        logging.debug(
            f"...Finished setting up Infra(tag={tag}) for Node({self.node_id})"
        )
        return result

    def destroy(self, tag: InfraTag = "default") -> NodeDestroyResult:
        return asyncio.get_event_loop().run_until_complete(self._destroy_async(tag=tag))

    async def _destroy_async(self, *, tag: InfraTag) -> NodeDestroyResult:
        logging.debug(f"Tearing down infrastructure for Node({self.node_id})...")
        result = await self._infra_actor.destroy.remote(processors=self._processors)
        logging.debug(
            f"...Finished tearing down infrastructure for Node({self.node_id})"
        )
        return result
