import asyncio
import logging
import signal
from functools import wraps
import inspect
from typing import List, Optional

from buildflow import utils
from buildflow.api import (
    NodeAPI,
    NodeApplyResult,
    NodeDestroyResult,
    NodePlan,
    SinkType,
    SourceType,
    NodeID,
)
from buildflow.core.infra import PulumiInfraActor
from buildflow.core.infra.config import InfraConfig
from buildflow.core.processor import Processor
from buildflow.core.runtime import RuntimeActor
from buildflow.core.runtime.config import RuntimeConfig, ReplicaConfig
from buildflow.io.registry import EmptySink


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
    num_concurrent_tasks: int = 1,
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
            num_concurrent_tasks=num_concurrent_tasks,
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
    ) -> None:
        self.node_id = node_id
        self._processors: List[Processor] = []
        # The Node class is a wrapper around the Runtime and Infrastructure
        self._runtime_config = runtime_config or RuntimeConfig.IO_BOUND()
        self._runtime_actor = None
        self._infra_config = infra_config or InfraConfig.DEFAULT()

    def processor(
        self,
        source: SourceType,
        sink: Optional[SinkType] = None,
        num_cpus: float = 1.0,
        num_concurrent_tasks: int = 1,
        log_level: str = None,
    ):
        # NOTE: processor_decorator is a function that returns an Ad Hoc
        # Processor implementation.
        return _processor_decorator(
            node=self,
            source=source,
            sink=sink,
            num_cpus=num_cpus,
            num_concurrent_tasks=num_concurrent_tasks,
            log_level=log_level,
        )

    def add(
        self,
        processor: Processor,
        *,
        num_cpus: float = 1.0,
        num_concurrent_tasks: int = 1,
        log_level: str = None,
    ):
        if self._runtime_actor is not None and self._runtime_actor.is_active():
            raise RuntimeError("Cannot add processor to running node.")
        if processor.processor_id in self._runtime_config.replica_configs:
            raise RuntimeError(
                f"Processor({processor.processor_id}) already exists in node."
            )
        if log_level is None:
            log_level = self._runtime_config.log_level

        # Each processor gets its own replica config
        self._runtime_config.replica_configs[processor.processor_id] = ReplicaConfig(
            num_cpus=num_cpus,
            num_concurrent_tasks=num_concurrent_tasks,
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
    ):
        coro = self._run_async(
            disable_usage_stats=disable_usage_stats,
            apply_infrastructure=apply_infrastructure,
            destroy_infrastructure=destroy_infrastructure,
            debug_run=debug_run,
        )
        if block_runtime:
            asyncio.get_event_loop().run_until_complete(coro)
        else:
            return coro

    async def _run_async(
        self,
        *,
        disable_usage_stats: bool = False,
        apply_infrastructure: bool = False,
        destroy_infrastructure: bool = False,
        debug_run: bool = False,
    ):
        self._runtime_actor = RuntimeActor.remote(self._runtime_config)
        # BuildFlow Usage Stats
        if not disable_usage_stats:
            utils.log_buildflow_usage()
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
        logging.info(f"Draining Node({self.node_id})...")
        await self._runtime_actor.drain.remote()
        logging.info(f"...Finished draining Node({self.node_id})")
        if destroy:
            await self._destroy_async()
        return True

    def plan(self) -> NodePlan:
        return asyncio.get_event_loop().run_until_complete(self._plan_async())

    async def _plan_async(self) -> NodePlan:
        logging.info(f"Planning infrastructure for Node({self.node_id})...")
        infra_actor = PulumiInfraActor.remote(
            self._infra_config, stack_name=self.node_id
        )
        result = await infra_actor.plan.remote(processors=self._processors)
        logging.info(f"...Finished planning infrastructure for Node({self.node_id})")
        return result

    def apply(self) -> NodeApplyResult:
        return asyncio.get_event_loop().run_until_complete(self._apply_async())

    async def _apply_async(self) -> NodeApplyResult:
        logging.info(f"Setting up infrastructure for Node({self.node_id})...")
        infra_actor = PulumiInfraActor.remote(
            self._infra_config, stack_name=self.node_id
        )
        result = await infra_actor.apply.remote(processors=self._processors)
        logging.info(f"...Finished setting up infrastructure for Node({self.node_id})")
        return result

    def destroy(self) -> NodeDestroyResult:
        return asyncio.get_event_loop().run_until_complete(self._destroy_async())

    async def _destroy_async(self) -> NodeDestroyResult:
        logging.info(f"Tearing down infrastructure for Node({self.node_id})...")
        infra_actor = PulumiInfraActor.remote(
            self._infra_config, stack_name=self.node_id
        )
        result = await infra_actor.destroy.remote(processors=self._processors)
        logging.info(
            f"...Finished tearing down infrastructure for Node({self.node_id})"
        )
        return result
