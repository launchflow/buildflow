import logging
import signal
from functools import wraps
import inspect
from typing import List, Optional

import ray
import asyncio

from buildflow import utils
from buildflow.api import (
    NodeAPI,
    NodeApplyResult,
    NodeDestroyResult,
    NodePlan,
    SinkType,
    SourceType,
)
from buildflow.core.infra import PulumiInfraActor
from buildflow.core.infra.config import InfraConfig
from buildflow.core.processor import Processor
from buildflow.core.runtime import RuntimeActor
from buildflow.core.runtime.config import RuntimeConfig
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


# NOTE: Node implements NodeAPI, which is a combination of RuntimeAPI and InfraAPI.
class Node(NodeAPI):
    def __init__(
        self,
        name: str = "",
        runtime_config: RuntimeConfig = RuntimeConfig.DEBUG(),
        infra_config: InfraConfig = InfraConfig.DEBUG(),
    ) -> None:
        self.name = name
        self._processors: List[Processor] = []
        # The Node class is a wrapper around the Runtime and Infra
        self._runtime = RuntimeActor.remote(runtime_config)
        self._infra = PulumiInfraActor.remote(infra_config)

    def processor(self, source: SourceType, sink: Optional[SinkType] = None, **kwargs):
        # NOTE: processor_decorator is a function that returns an Ad Hoc
        # Processor implementation.
        return processor_decorator(node=self, source=source, sink=sink, **kwargs)

    def add(self, processor: Processor):
        is_active = ray.get(self._runtime.is_active.remote())
        if is_active:
            raise RuntimeError("Cannot add processor to running node.")
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
        if not disable_usage_stats:
            utils.log_buildflow_usage()

        if apply_infrastructure:
            self.apply(debug_run=debug_run)

        # handle interupt signals
        signal.signal(
            signal.SIGTERM,
            lambda x, y: self.drain(destroy=destroy_infrastructure),
        )
        signal.signal(
            signal.SIGINT,
            lambda x, y: self.drain(destroy=destroy_infrastructure),
        )

        # start the runtime and (optionally) block until it's done
        ray.get(self._runtime.run.remote(processors=self._processors))
        remote_runtime_task = self._runtime.run_until_complete.remote()

        if destroy_infrastructure:
            runtime_future: asyncio.Future = asyncio.wrap_future(
                remote_runtime_task.future()
            )
            # schedule the destroy to run after the runtime is done.
            # NOTE: This will only run if the runtime finishes successfully.
            runtime_future.add_done_callback(lambda _: self.destroy())

        if block_runtime:
            logging.info("Waiting for runtime to finish...")
            ray.get(remote_runtime_task)

    def drain(self, destroy: bool = False):
        logging.info(f"Draining Node({self.name})...")
        ray.get(self._runtime.drain.remote())
        logging.info(f"...Finished draining Node({self.name})")
        if destroy:
            self.destroy()
        return True

    def plan(self) -> NodePlan:
        return ray.get(self._infra.plan.remote(processors=self._processors))

    def apply(self, debug_run: bool = False) -> NodeApplyResult:
        logging.info(f"Setting up infra for Node({self.name})...")
        result = ray.get(self._infra.apply.remote(processors=self._processors))
        logging.info(f"...Finished setting up infra for Node({self.name})")
        return result

    def destroy(self) -> NodeDestroyResult:
        logging.info(f"Tearing down infra for Node({self.name})...")
        result = ray.get(self._infra.destroy.remote(processors=self._processors))
        logging.info(f"...Finished tearing down infra for Node({self.name})")
        return result
