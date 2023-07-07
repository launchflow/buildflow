from dataclasses import dataclass
from typing import Iterable

from buildflow.api.node import NodeAPI, NodePlan


@dataclass
class GridPlan:
    name: str
    nodes: Iterable[NodePlan]


class GridNode:
    def __init__(self, name: str, cluster_address: str, node: NodeAPI):
        self.name = name
        self.cluster_address = cluster_address
        self.node = node


class GridAPI:
    def add_node(self, node: NodeAPI):
        raise NotImplementedError("add_node not implemented")

    def deploy(self):
        raise NotImplementedError("deploy not implemented")

    def plan(self):
        raise NotImplementedError("plan not implemented")

    def setup(self):
        raise NotImplementedError("setup not implemented")
