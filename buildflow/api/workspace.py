from typing import List

from buildflow.api.project import ProjectAPI


WorkspaceID = str


class WorkspaceAPI:
    workspace_id: WorkspaceID

    def get_or_create_project(self) -> ProjectAPI:
        """Creates a new project in the workspace."""
        raise NotImplementedError("create_project not implemented")

    def list_projects(self) -> List[ProjectAPI]:
        """Returns all projects in the workspace."""
        raise NotImplementedError("get_projects not implemented")

    @classmethod
    def create(cls, base_dir: str) -> "WorkspaceAPI":
        """Creates a new workspace at the given directory."""
        raise NotImplementedError("create not implemented")

    @classmethod
    def load(cls, base_dir: str) -> "WorkspaceAPI":
        """Loads the workspace from the given directory."""
        raise NotImplementedError("load not implemented")

    def save(self):
        """Saves the workspace to the given directory."""
        raise NotImplementedError("save not implemented")
