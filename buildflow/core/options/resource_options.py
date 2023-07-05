import dataclasses
import enum
from buildflow.core.options._options import Options


@dataclasses.dataclass
class AWSResourceOptions(Options):
    default_region: str

    @classmethod
    def default(cls) -> "AWSResourceOptions":
        return cls(default_region="us-east-1")


@dataclasses.dataclass
class AzureResourceOptions(Options):
    default_region: str

    @classmethod
    def default(cls) -> "AzureResourceOptions":
        return cls(default_region="eastus")


@dataclasses.dataclass
class GCPResourceOptions(Options):
    default_project_id: str
    default_region: str
    default_zone: str

    @classmethod
    def default(cls) -> "GCPResourceOptions":
        return cls(
            default_project_id="daring-runway-374503",
            default_region="us-central1",
            default_zone="us-central1-a",
        )


@dataclasses.dataclass
class LocalResourceOptions(Options):
    @classmethod
    def default(cls) -> "LocalResourceOptions":
        return cls()


class ResourceProvider(enum.Enum):
    AWS = "aws"
    AZURE = "azure"
    GCP = "gcp"
    LOCAL = "local"


@dataclasses.dataclass
class ResourceOptions(Options):
    resource_provider: ResourceProvider
    # Options for each resource provider
    aws: AWSResourceOptions
    azure: AzureResourceOptions
    gcp: GCPResourceOptions
    local: LocalResourceOptions

    @classmethod
    def default(cls) -> "ResourceOptions":
        return cls(
            resource_provider=ResourceProvider.GCP,
            aws=AWSResourceOptions.default(),
            azure=AzureResourceOptions.default(),
            gcp=GCPResourceOptions.default(),
            local=LocalResourceOptions.default(),
        )
