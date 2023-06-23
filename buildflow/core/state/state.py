import dataclasses
import hashlib
import inspect
import json
import logging
import os
import time
from typing import Any, Optional, Dict, List
from uuid import uuid4
from buildflow import utils
import requests
from buildflow.api.node import NodeID


BUILDFLOW_STATE_DIR_LOCAL = os.path.join(
    os.path.expanduser("~"), ".config", "buildflow", "state"
)

PULUMI_STATE_DIR_LOCAL = os.path.join(
    os.path.expanduser("~"), ".config", "buildflow", "state"
)


def _write_json_file(file_path: str, data: Any):
    if not os.path.exists(os.path.dirname(file_path)):
        os.makedirs(os.path.dirname(file_path))
    json.dump(data, open(file_path, "w"), indent=4)


def _read_json_file(file_path: str) -> Dict[str, Any]:
    if not os.path.exists(file_path):
        return {}
    return json.load(open(file_path, "r"))


@dataclasses.dataclass
class RuntimeState:
    pass


@dataclasses.dataclass
class InfraState:
    pass


# Do we want to include timestamps in the state?
@dataclasses.dataclass
class NodeState:
    # NodeState is *only* a container (directory) for runtime and infra state
    id_: NodeID
    # RuntimeState is a snapshot of the runtime state for the node
    runtime: RuntimeState
    # InfraState is a snapshot of the infra state for the node
    infra: InfraState

    def write(self, state_dir: str):
        node_state_dir = os.path.join(state_dir, "nodes", self.id_)
        # write the runtime state to the node state dir
        runtime_state_file_path = os.path.join(node_state_dir, "runtime.json")
        _write_json_file(runtime_state_file_path, dataclasses.asdict(self.runtime))
        # write the infra state to the node state dir
        infra_state_file_path = os.path.join(node_state_dir, "infra.json")
        _write_json_file(infra_state_file_path, dataclasses.asdict(self.infra))

    @classmethod
    def read(cls, state_dir: str, node_id: NodeID) -> Optional["NodeState"]:
        node_state_dir = os.path.join(state_dir, "nodes", node_id)
        if not os.path.exists(node_state_dir):
            return None
        # reads the runtime state from the node state dir
        runtime_state_file_path = os.path.join(node_state_dir, "runtime.json")
        runtime_state = RuntimeState(**_read_json_file(runtime_state_file_path))
        # reads the infra state from the node state dir
        infra_state_file_path = os.path.join(node_state_dir, "infra.json")
        infra_state = InfraState(**_read_json_file(infra_state_file_path))
        return cls(node_id, runtime_state, infra_state)


@dataclasses.dataclass
class State:
    state_dir: str = BUILDFLOW_STATE_DIR_LOCAL
    # _nodes is a cache of all node states for the current session
    _nodes: Dict[NodeID, NodeState] = dataclasses.field(init=False)

    def __post_init__(self):
        if not os.path.exists(self.state_dir):
            os.makedirs(self.state_dir)

    def get_node_state(self, node_id: NodeID) -> Optional[NodeState]:
        # first we check if its in the node state cache
        if node_id in self._nodes:
            return self._nodes[node_id]
        # then we check if its in the state dir, and if not, we return None
        return NodeState.read(self.state_dir, node_id)

    def set_node_state(self, node_id: NodeID, node_state: NodeState):
        # we save the node state to the state dir
        node_state.write(self.state_dir)
        # we also update the node state cache
        self._nodes[node_id] = node_state
