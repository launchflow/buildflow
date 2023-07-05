from typing import Dict

from buildflow.api.workspaces.workspace.projects.project import ProjectAPI, ProjectID

WorkspaceID = str


class WorkspaceAPI:
    workspace_id: WorkspaceID
    projects: Dict[ProjectID, ProjectAPI]

    @classmethod
    def create(cls, workspace_dir: str) -> "WorkspaceAPI":
        """Creates a new workspace at the given directory."""
        raise NotImplementedError("create not implemented")

    @classmethod
    def load(cls, workspace_dir: str) -> "WorkspaceAPI":
        """Loads the workspace from the given directory."""
        raise NotImplementedError("load not implemented")

    def save(self):
        """Saves the workspace to the given directory."""
        raise NotImplementedError("save not implemented")
