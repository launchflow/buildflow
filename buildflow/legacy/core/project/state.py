import dataclasses
import os
from typing import Dict, Optional

from buildflow import utils
from buildflow.api.node import NodeID
from buildflow.api.shared import State
from buildflow.core.node.state import NodeState
from buildflow.core.workspace.pulumi.workspace import PulumiWorkspace
from buildflow.core.runtime.state import RuntimeState
from buildflow.core.infra.state import InfraState


@dataclasses.dataclass
class ProjectState(State):
    node_states: Dict[NodeID, NodeState]
    pulumi_workspace: Optional[PulumiWorkspace]

    def set_runtime_state(self, node_id: NodeID, runtime_state: RuntimeState):
        node_state = self.get_or_create_node_state(node_id)
        node_state.set_runtime_state(runtime_state)

    def set_infra_state(self, node_id: NodeID, infra_state: InfraState):
        node_state = self.get_or_create_node_state(node_id)
        node_state.set_infra_state(infra_state)

    def get_or_create_node_state(self, node_id: NodeID) -> NodeState:
        node_state = self._get_node_state(node_id)
        if node_state is None:
            node_state = self._create_node_state(node_id)
        return node_state

    def _create_node_state(self, node_id: NodeID) -> NodeState:
        node_state = NodeState.initial()
        self.node_states[node_id] = node_state
        return node_state

    def _get_node_state(self, node_id: NodeID) -> Optional[NodeState]:
        return self.node_states.get(node_id, None)

    @classmethod
    def initial(cls) -> "ProjectState":
        return ProjectState(node_states={}, pulumi_workspace=None)

    @classmethod
    def load(cls, project_state_dir: str) -> "ProjectState":
        utils.assert_path_exists(project_state_dir)
        # load the node states
        node_states_dir = os.path.join(project_state_dir, "nodes")
        node_states = {}
        for node_id in os.listdir(node_states_dir):
            node_state_path = os.path.join(node_states_dir, node_id)
            node_states[node_id] = NodeState.load(node_state_path)
        # load the pulumi state (if it exists)
        pulumi_state = None
        if os.path.exists(os.path.join(project_state_dir, "pulumi")):
            pulumi_workspace_dir = os.path.join(project_state_dir, "pulumi")
            pulumi_state = PulumiWorkspace.load(pulumi_workspace_dir)
        # return the project state
        return cls(node_states, pulumi_state)

    def dump(self, project_state_dir: str):
        # dump the node states
        node_state_dir = os.path.join(project_state_dir, "nodes")
        for node_id, node_state in self.node_states.items():
            node_state_path = os.path.join(node_state_dir, node_id)
            node_state.dump(node_state_path)
        # save the pulumi workspace (if it exists)
        if self.pulumi_workspace is not None:
            self.pulumi_workspace.save()
