from typing import Dict

from buildflow.api_v2.deployment import DeploymentAPI, DeploymentID
from buildflow.api_v2.project.project_config import ProjectConfig


ProjectID = str


class ProjectAPI:
    project_id: ProjectID
    project_config: ProjectConfig
    deployments: Dict[DeploymentID, DeploymentAPI]

    @classmethod
    def create(cls, project_dir: str) -> "ProjectAPI":
        """Creates a new project in the given directory."""
        raise NotImplementedError("create not implemented")

    @classmethod
    def load(cls, project_dir: str) -> "ProjectAPI":
        """Loads the project from the given directory."""
        raise NotImplementedError("load not implemented")
