from typing import Dict

from buildflow.api.node import NodeAPI
from buildflow.utils import uuid


class GridNode:
    def __init__(self, name: str, cluster_address: str, node: NodeAPI):
        self.name = name
        self.cluster_address = cluster_address
        self.node = node


class GridAPI:
    def __init__(self):
        self.nodes: Dict[str, NodeAPI] = {}

    def add_node(
        self,
        node: NodeAPI,
        name: str = "",
        cluster_address: str = "",
    ):
        if not name:
            if node.name:
                name = node.name
            else:
                name = uuid()
        try:
            self.nodes[name] = GridNode(name, cluster_address, node)
        except KeyError:
            raise ValueError(
                f"node with name {name} alreay exists in grid. "
                "all node names must be unique."
            )
