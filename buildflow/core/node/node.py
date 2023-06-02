import asyncio
from functools import wraps
import inspect
from typing import List, Optional

import signal
import logging

from buildflow.api import (
    NodeAPI,
    NodeApplyResult,
    NodeDestroyResult,
    NodePlan,
    SinkType,
    SourceType,
)
from buildflow.core.node import _utils as node_utils
from buildflow.core.infrastructure import InfrastructureActor
from buildflow.core.processor import Processor
from buildflow.core.runtime import RuntimeActor
from buildflow.io.registry import EmptySink
from buildflow import utils
from buildflow.core.runtime.config import RuntimeConfig


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


def processor_decorator(
    node: "Node",
    source: SourceType,
    sink: Optional[SinkType] = None,
    *,
    num_cpus: float = 1.0,
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
        processor_instance = _AdHocProcessor(name=processor_id)
        node.add(processor_instance)

        return processor_instance

    return decorator_function


# NOTE: Node implements NodeAPI, which is a combination of RuntimeAPI and
# InfrastructureAPI.
class Node(NodeAPI):
    def __init__(
        self,
        name: str = "",
        runtime_config: RuntimeConfig = RuntimeConfig.DEBUG(),
        *,
        log_level: str = "INFO",
    ) -> None:
        self.name = name
        self._processors: List[Processor] = []
        # The Node class is a wrapper around the Runtime and Infrastructure
        self._runtime_config = runtime_config
        self._runtime = None
        self._infrastructure = InfrastructureActor.remote(log_level=log_level)

    def processor(self, source: SourceType, sink: Optional[SinkType] = None, **kwargs):
        # NOTE: processor_decorator is a function that returns an Ad Hoc
        # Processor implementation.
        return processor_decorator(node=self, source=source, sink=sink, **kwargs)

    def add(self, processor: Processor):
        if self._runtime is not None:
            raise RuntimeError("Cannot add processor to running node.")
        self._processors.append(processor)

    def plan(self) -> NodePlan:
        return asyncio.get_event_loop().run_until_complete(self._plan_async())

    async def _plan_async(self) -> NodePlan:
        return await self._infrastructure.plan.remote(processors=self._processors)

    def run(
        self,
        *,
        disable_usage_stats: bool = False,
        disable_resource_creation: bool = True,
        blocking: bool = True,
        debug_run: bool = False,
    ):
        coro = self._run_async(
            disable_usage_stats=disable_usage_stats,
            disable_resource_creation=disable_resource_creation,
            debug_run=debug_run,
        )
        if blocking:
            asyncio.get_event_loop().run_until_complete(coro)
        else:
            return coro

    async def _run_async(
        self,
        *,
        disable_usage_stats: bool = False,
        disable_resource_creation: bool = True,
        debug_run: bool = False,
    ):
        self._runtime = RuntimeActor.remote(self._runtime_config)

        # BuildFlow Usage Stats
        if not disable_usage_stats:
            node_utils.log_buildflow_usage()
        # BuildFlow Resource Creation
        if not disable_resource_creation:
            await self._apply_async()
        # BuildFlow Runtime
        # schedule cleanup
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self._drain()))
        # start the runtime
        await self._runtime.run.remote(processors=self._processors)
        return await self._runtime.run_until_complete.remote()

    async def _drain(self, *args, **kwargs):
        logging.info(f"Draining Node({self.name})...")
        await self._runtime.drain.remote()
        logging.info(f"...Finished draining Node({self.name})")
        return True

    def apply(self) -> NodeApplyResult:
        return asyncio.get_event_loop().run_until_complete(self._apply_async())

    async def _apply_async(self) -> NodeApplyResult:
        logging.info(f"Setting up infrastructure for Node({self.name})...")
        result = await self._infrastructure.apply.remote(processors=self._processors)
        logging.info(f"...Finished setting up infrastructure for Node({self.name})")
        return result

    def destroy(self) -> NodeDestroyResult:
        return asyncio.get_event_loop().run_until_complete(self._destroy_async())

    async def _destroy_async(self) -> NodeDestroyResult:
        logging.info(f"Tearing down infrastructure for Node({self.name})...")
        result = await self._infrastructure.destroy.remote(processors=self._processors)
        logging.info(f"...Finished tearing down infrastructure for Node({self.name})")
        return result
