from buildflow.api.workspaces.workspace.projects._shared.config import Config
from buildflow.api.workspaces.workspace.projects.project.config.aws.config import (
    AWSResourceConfig,
)
from buildflow.api.workspaces.workspace.projects.project.config.azure.config import (
    AzureResourceConfig,
)
from buildflow.api.workspaces.workspace.projects.project.config.gcp.config import (
    GCPResourceConfig,
)
from buildflow.api.workspaces.workspace.projects.project.config.local.config import (
    LocalResourceConfig,
)


class ProjectConfig(Config):
    local: LocalResourceConfig
    gcp: GCPResourceConfig
    aws: AWSResourceConfig
    azure: AzureResourceConfig
