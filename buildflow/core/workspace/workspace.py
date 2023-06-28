import dataclasses
import os

from buildflow import utils
from buildflow.api.node import NodeID
from buildflow.api.project import ProjectID
from buildflow.api.workspace import WorkspaceAPI
from buildflow.core.node.state import NodeState
from buildflow.core.workspace.state import WorkspaceState
from buildflow.core.runtime.state import RuntimeState
from buildflow.core.infra.state import InfraState

BUILDFLOW_WORKSPACE_DIR = os.path.join(os.path.expanduser("~"), ".config", "buildflow")


@dataclasses.dataclass
class Workspace(WorkspaceAPI):
    workspace_dir: str
    workspace_state: WorkspaceState

    def set_runtime_state(
        self, *, project_id: ProjectID, node_id: NodeID, runtime_state: RuntimeState
    ):
        project_state = self.workspace_state.get_or_create_project_state(project_id)
        project_state.set_runtime_state(node_id, runtime_state)
        self.save()

    def set_infra_state(
        self, *, project_id: ProjectID, node_id: NodeID, infra_state: InfraState
    ):
        project_state = self.workspace_state.get_or_create_project_state(project_id)
        project_state.set_infra_state(node_id, infra_state)
        self.save()

    def get_or_create_node_state(
        self, *, project_id: ProjectID, node_id: NodeID
    ) -> NodeState:
        project_state = self.workspace_state.get_or_create_project_state(project_id)
        return project_state.get_or_create_node_state(node_id)

    @classmethod
    def create(cls, workspace_dir: str) -> "Workspace":
        # create the initial workspace state
        workspace_state_dir = os.path.join(workspace_dir, "state")
        workspace_state = WorkspaceState.initial()
        workspace_state.dump(workspace_state_dir)
        # return the workspace
        return cls(workspace_dir, workspace_state)

    @classmethod
    def load(cls, workspace_dir: str) -> "Workspace":
        utils.assert_path_exists(workspace_dir)
        # load the workspace state
        workspace_state_dir = os.path.join(workspace_dir, "state")
        workspace_state = WorkspaceState.load(workspace_state_dir)
        # return the workspace
        return cls(workspace_dir, workspace_state)

    def save(self):
        # save the workspace state
        workspace_state_dir = os.path.join(self.workspace_dir, "state")
        self.workspace_state.dump(workspace_state_dir)


_WORKSPACE = None


def get_or_create_workspace() -> Workspace:
    global _WORKSPACE
    if _WORKSPACE is None:
        try:
            _WORKSPACE = Workspace.load(BUILDFLOW_WORKSPACE_DIR)
        except ValueError:
            _WORKSPACE = Workspace.create(BUILDFLOW_WORKSPACE_DIR)
    return _WORKSPACE
