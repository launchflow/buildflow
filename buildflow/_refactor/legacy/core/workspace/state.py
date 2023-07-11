import dataclasses
import os
from typing import Dict, Optional

from buildflow.api.project import ProjectID
from buildflow.api.shared import State
from buildflow.core import utils
from buildflow.core.project.state import ProjectState


@dataclasses.dataclass
class WorkspaceState(State):
    project_states: Dict[ProjectID, ProjectState]

    def get_or_create_project_state(self, project_id: ProjectID) -> ProjectState:
        project_state = self._get_project_state(project_id)
        if project_state is None:
            project_state = self._create_project_state(project_id)
        return project_state

    def _create_project_state(self, project_id: ProjectID) -> ProjectState:
        project_state = ProjectState.initial()
        self.project_states[project_id] = project_state
        return project_state

    def _get_project_state(self, project_id: ProjectID) -> Optional[ProjectState]:
        return self.project_states.get(project_id, None)

    @classmethod
    def initial(cls) -> "WorkspaceState":
        return cls(project_states={})

    @classmethod
    def load(cls, workspace_state_dir: str) -> "WorkspaceState":
        utils.assert_path_exists(workspace_state_dir)
        # load the project workspaces
        project_states_dir = os.path.join(workspace_state_dir, "projects")
        project_states = {}
        for project_id in os.listdir(project_states_dir):
            project_state_dir = os.path.join(project_states_dir, project_id)
            project_states[project_id] = ProjectState.load(project_state_dir)
        # return the workspace state
        return cls(project_states)

    def dump(self, workspace_state_dir: str):
        # dump the project states
        project_states_dir = os.path.join(workspace_state_dir, "projects")
        for project_id, project_state in self.project_states.items():
            project_state_dir = os.path.join(project_states_dir, project_id)
            project_state.dump(project_state_dir)
