from typing import List

import ray

from buildflow.api import NodeAPI, NodeApplyResult, NodeDestroyResult, NodePlan
from buildflow.core.node import _utils as node_utils
from buildflow.core.node.infrastructure import Infrastructure
from buildflow.core.processor import Processor
from buildflow.core.runtime import Environment, Runtime, RuntimeActor


# NOTE: Node implements NodeAPI, which is a combination of RuntimeAPI and
# InfrastructureAPI.
class Node(NodeAPI):

    def __init__(self, name: str = "") -> None:
        self.name = name
        self._processors: List[Processor] = []
        # The Node class is a wrapper around the Runtime and Infrastructure
        self._runtime = RuntimeActor.remote()
        self._infrastructure = Infrastructure()

    def add(self, processor: Processor):
        if self._runtime.is_active.remote():
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
    ) -> Runtime:
        # BuildFlow Usage Stats
        if not disable_usage_stats:
            node_utils.log_buildflow_usage()
        # BuildFlow Resource Creation
        if not disable_resource_creation:
            self.apply()
        # BuildFlow Runtime
        run_result: Runtime = ray.get(
            self._runtime.run.remote(processors=self._processors,
                                     env=Environment()))
        if blocking:
            run_result.run_until_complete()
        return run_result

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
