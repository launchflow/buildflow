from typing import List

from buildflow.api_v2.node.infra import InfraAPI
from buildflow.api_v2.node.pattern import PatternAPI
from buildflow.api_v2.node.runtime import RuntimeAPI

NodeID = str


class NodeAPI:
    node_id: NodeID
    runtime: RuntimeAPI
    infra: InfraAPI
    patterns: List[PatternAPI]

    def add(self, pattern: PatternAPI):
        raise NotImplementedError("add not implemented")

    def plan(self):
        raise NotImplementedError("plan not implemented")

    def run(self):
        raise NotImplementedError("run not implemented")

    def apply(self):
        raise NotImplementedError("apply not implemented")

    def destroy(self):
        raise NotImplementedError("destroy not implemented")
