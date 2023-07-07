import dataclasses
import enum

from buildflow.core.options._options import Options
from buildflow.core.types.gcp_types import ProjectID, Region, Zone


@dataclasses.dataclass
class AWSOptions(Options):
    default_region: str

    @classmethod
    def default(cls) -> "AWSOptions":
        return cls(default_region="us-east-1")


@dataclasses.dataclass
class AzureOptions(Options):
    default_region: str

    @classmethod
    def default(cls) -> "AzureOptions":
        return cls(default_region="eastus")


@dataclasses.dataclass
class GCPOptions(Options):
    default_project_id: ProjectID
    default_region: Region
    default_zone: Zone

    @classmethod
    # DO NOT SUBMIT: Remove these defaults and replace with a Config type
    def default(cls) -> "GCPOptions":
        return cls(
            default_project_id="daring-runway-374503",
            default_region="us-central1",
            default_zone="us-central1-a",
        )


@dataclasses.dataclass
class LocalOptions(Options):
    @classmethod
    def default(cls) -> "LocalOptions":
        return cls()


class CloudProvider(enum.Enum):
    AWS = "aws"
    AZURE = "azure"
    GCP = "gcp"
    LOCAL = "local"


@dataclasses.dataclass
class PrimitiveOptions(Options):
    cloud_provider: CloudProvider
    # Options for each resource provider
    aws: AWSOptions
    azure: AzureOptions
    gcp: GCPOptions
    local: LocalOptions

    @classmethod
    def default(cls) -> "PrimitiveOptions":
        return cls(
            cloud_provider=CloudProvider.GCP,
            aws=AWSOptions.default(),
            azure=AzureOptions.default(),
            gcp=GCPOptions.default(),
            local=LocalOptions.default(),
        )
