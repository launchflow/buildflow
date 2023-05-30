from typing import List, Optional

import ray
import signal

from buildflow.api import (NodeAPI, NodeApplyResult, NodeDestroyResult,
                           NodePlan, SinkType, SourceType)
from buildflow.core.node import _utils as node_utils
from buildflow.core.node.infrastructure import Infrastructure
from buildflow.core.processor import Processor
from buildflow.core.runtime import RuntimeActor
from buildflow.io.registry import EmptySink
from buildflow import utils
from buildflow.core.runtime.config import RuntimeConfig


def processor_decorator(
    node: 'Node',
    source: SourceType,
    sink: Optional[SinkType] = None,
    *,
    num_cpus: float = 0.5,
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
            (Processor, ),
            {
                "source": lambda self: source,
                "sink": lambda self: sink,
                "sinks": lambda self: [],
                "setup": lambda self: None,
                "process": lambda self, payload: original_function(payload),
                "__call__": wrapper_function,
            },
        )
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
        runtime_config: RuntimeConfig = RuntimeConfig.DEBUG()
    ) -> None:
        self.name = name
        self._processors: List[Processor] = []
        # The Node class is a wrapper around the Runtime and Infrastructure
        self._runtime = RuntimeActor.remote(runtime_config)
        self._infrastructure = Infrastructure()

    def processor(self,
                  source: SourceType,
                  sink: Optional[SinkType] = None,
                  **kwargs):
        # NOTE: processor_decorator is a function that returns an Ad Hoc
        # Processor implementation.
        return processor_decorator(node=self,
                                   source=source,
                                   sink=sink,
                                   **kwargs)

    def add(self, processor: Processor):
        is_active = ray.get(self._runtime.is_active.remote())
        if is_active:
            raise RuntimeError("Cannot add processor to running node.")
        self._processors.append(processor)

    def plan(self) -> NodePlan:
        return self._infrastructure.plan(node_name=self.name,
                                         node_processors=self._processors)

    def run(
        self,
        *,
        disable_usage_stats: bool = False,
        disable_resource_creation: bool = True,
        blocking: bool = True,
        debug_run: bool = False,
    ):
        # BuildFlow Usage Stats
        if not disable_usage_stats:
            node_utils.log_buildflow_usage()
        # BuildFlow Resource Creation
        if not disable_resource_creation:
            self.apply()
        # BuildFlow Runtime
        # schedule cleanup
        signal.signal(signal.SIGTERM, self.drain)
        signal.signal(signal.SIGINT, self.drain)
        ray.get(self._runtime.start.remote(processors=self._processors))
        if blocking:
            ray.get(self._runtime.run_until_complete.remote())

    def drain(self, *args, **kwargs):
        print("Draining node...")
        ray.get(self._runtime.drain.remote())
        print("...Finished draining node")
        return True

    def apply(self) -> NodeApplyResult:
        print("Setting up resources...")
        result = self._infrastructure.apply(node_name=self.name,
                                            node_processors=self._processors)
        print("...Finished setting up resources")
        return result

    def destroy(self) -> NodeDestroyResult:
        print("Tearing down resources...")
        result = self._infrastructure.destroy(node_name=self.name,
                                              node_processors=self._processors)
        print("...Finished tearing down resources")
        return result
