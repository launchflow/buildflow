import dataclasses
import enum
from typing import Optional

from buildflow.config._config import Config
from buildflow.core import utils
from buildflow.core.options._options import Options
from buildflow.core.types import gcp_types


@dataclasses.dataclass
class AWSOptions(Options):
    default_region: Optional[str]

    @classmethod
    def default(cls) -> "AWSOptions":
        return cls(default_region=None)


@dataclasses.dataclass
class AzureOptions(Options):
    default_region: Optional[str]

    @classmethod
    def default(cls) -> "AzureOptions":
        return cls(default_region=None)


@dataclasses.dataclass
class GCPOptions(Options):
    default_project_id: Optional[gcp_types.GCPProjectID]
    default_region: Optional[gcp_types.GCPRegion]
    default_zone: Optional[gcp_types.GCPZone]

    # TODO: Use gcloud cli to fetch defaults
    @classmethod
    def default(cls) -> "GCPOptions":
        return cls(
            default_project_id=None,
            default_region=None,
            default_zone=None,
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
class CloudProviderConfig(Config):
    default_cloud_provider: CloudProvider
    # Options for each resource provider
    aws_options: AWSOptions
    azure_options: AzureOptions
    gcp_options: GCPOptions
    local_options: LocalOptions

    @classmethod
    def default(cls) -> "CloudProviderConfig":
        return cls(
            default_cloud_provider=CloudProvider.GCP,
            aws_options=AWSOptions.default(),
            azure_options=AzureOptions.default(),
            gcp_options=GCPOptions.default(),
            local_options=LocalOptions.default(),
        )

    @classmethod
    def load(cls, cloud_provider_config_path: str) -> "CloudProviderConfig":
        config_dict = utils.read_yaml_file(cloud_provider_config_path)
        return cls(
            default_cloud_provider=CloudProvider(config_dict["default_cloud_provider"]),
            aws_options=AWSOptions(**config_dict["aws"]),
            azure_options=AzureOptions(**config_dict["azure"]),
            gcp_options=GCPOptions(**config_dict["gcp"]),
            local_options=LocalOptions(**config_dict["local"]),
        )

    def dump(self, cloud_provider_config_path: str):
        config_dict = {
            "default_cloud_provider": self.default_cloud_provider.value,
            "aws": dataclasses.asdict(self.aws_options),
            "azure": dataclasses.asdict(self.azure_options),
            "gcp": dataclasses.asdict(self.gcp_options),
            "local": dataclasses.asdict(self.local_options),
        }
        utils.write_yaml_file(cloud_provider_config_path, config_dict)
