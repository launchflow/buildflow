from buildflow.api_v2.config import ConfigAPI


class AWSResourceConfig(ConfigAPI):
    default_region: str


class AzureResourceConfig(ConfigAPI):
    default_region: str


class GCPResourceConfig(ConfigAPI):
    default_project_id: str
    default_region: str
    default_zone: str


class LocalResourceConfig(ConfigAPI):
    pass


class ProjectConfig(ConfigAPI):
    aws: AWSResourceConfig
    azure: AzureResourceConfig
    gcp: GCPResourceConfig
    local: LocalResourceConfig
