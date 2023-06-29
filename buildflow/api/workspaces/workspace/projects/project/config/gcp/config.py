from buildflow.api.workspaces.workspace.projects._shared.config import Config


class GCPResourceConfig(Config):
    default_project_id: str
    default_region: str
    default_zone: str
