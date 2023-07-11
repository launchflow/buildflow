import dataclasses
import os

from google.cloud.client import ClientWithProject

from buildflow.api.shared import Config
from buildflow.core import utils


# NOTE: This is currently unused, but we plan on using this when implementing local
# versions of the providers / resource types. i.e. Topic(local_config(use_redis=True))
@dataclasses.dataclass
class LocalResourceConfig(Config):
    @classmethod
    def default(cls) -> "LocalResourceConfig":
        return cls()

    @classmethod
    def load(cls, local_resource_config_path: str) -> "LocalResourceConfig":
        utils.assert_path_exists(local_resource_config_path)
        return cls()

    def dump(self, local_resource_config_path: str):
        utils.write_json_file(local_resource_config_path, {})


@dataclasses.dataclass
class GCPResourceConfig(Config):
    default_project_id: str
    default_region: str
    default_zone: str

    @classmethod
    def default(cls) -> "GCPResourceConfig":
        # NOTE: We use google's default client to get the default's for the current env.
        gcp_base_client = ClientWithProject()
        default_project_id = gcp_base_client.project
        if default_project_id is None:
            raise ValueError(
                "Could not load default GCP project ID. "
                "Please set the GOOGLE_CLOUD_PROJECT environment variable."
            )
        gcp_base_client.close()
        return cls(
            default_project_id=default_project_id,
            # TODO: make these configurable
            default_region="us-central1",
            default_zone="us-central1-a",
        )

    @classmethod
    def load(cls, gcp_resource_config_path: str) -> "GCPResourceConfig":
        utils.assert_path_exists(gcp_resource_config_path)
        # load the gcp resource config
        gcp_resource_config_dict = utils.read_json_file(gcp_resource_config_path)
        return cls(
            default_project_id=gcp_resource_config_dict["default_project_id"],
            default_region=gcp_resource_config_dict["default_region"],
            default_zone=gcp_resource_config_dict["default_zone"],
        )

    def dump(self, gcp_resource_config_path: str):
        # dump the gcp resource config
        gcp_resource_config_dict = {
            "default_project_id": self.default_project_id,
            "default_region": self.default_region,
            "default_zone": self.default_zone,
        }
        utils.write_json_file(gcp_resource_config_path, gcp_resource_config_dict)


@dataclasses.dataclass
class AWSResourceConfig(Config):
    default_region: str

    @classmethod
    def default(cls) -> "AWSResourceConfig":
        # TODO: make these configurable
        return cls(default_region="us-east-1")

    @classmethod
    def load(cls, aws_resource_config_path: str) -> "AWSResourceConfig":
        utils.assert_path_exists(aws_resource_config_path)
        # load the aws resource config
        aws_resource_config_dict = utils.read_json_file(aws_resource_config_path)
        return cls(
            default_region=aws_resource_config_dict["default_region"],
        )

    def dump(self, aws_resource_config_path: str):
        # dump the aws resource config
        aws_resource_config_dict = {
            "default_region": self.default_region,
        }
        utils.write_json_file(aws_resource_config_path, aws_resource_config_dict)


@dataclasses.dataclass
class AzureResourceConfig(Config):
    default_region: str

    @classmethod
    def default(cls) -> "AzureResourceConfig":
        # TODO: make these configurable
        return cls(default_region="eastus")

    @classmethod
    def load(cls, azure_resource_config_path: str) -> "AzureResourceConfig":
        utils.assert_path_exists(azure_resource_config_path)
        # load the azure resource config
        azure_resource_config_dict = utils.read_json_file(azure_resource_config_path)
        return cls(
            default_region=azure_resource_config_dict["default_region"],
        )

    def dump(self, azure_resource_config_path: str):
        # dump the azure resource config
        azure_resource_config_dict = {
            "default_region": self.default_region,
        }
        utils.write_json_file(azure_resource_config_path, azure_resource_config_dict)


@dataclasses.dataclass
class ResourcesConfig(Config):
    local: LocalResourceConfig
    gcp: GCPResourceConfig
    aws: AWSResourceConfig
    azure: AzureResourceConfig

    @classmethod
    def default(cls) -> "ResourcesConfig":
        return cls(
            local=LocalResourceConfig.default(),
            gcp=GCPResourceConfig.default(),
            aws=AWSResourceConfig.default(),
            azure=AzureResourceConfig.default(),
        )

    @classmethod
    def load(cls, resource_config_dir: str) -> "ResourcesConfig":
        utils.assert_path_exists(resource_config_dir)
        # load the local resource config
        local_resource_config_path = os.path.join(resource_config_dir, "local.json")
        local_resource_config = LocalResourceConfig.load(local_resource_config_path)
        # load the gcp resource config
        gcp_resource_config_path = os.path.join(resource_config_dir, "gcp.json")
        gcp_resource_config = GCPResourceConfig.load(gcp_resource_config_path)
        # load the aws resource config
        aws_resource_config_path = os.path.join(resource_config_dir, "aws.json")
        aws_resource_config = AWSResourceConfig.load(aws_resource_config_path)
        # load the azure resource config
        azure_resource_config_path = os.path.join(resource_config_dir, "azure.json")
        azure_resource_config = AzureResourceConfig.load(azure_resource_config_path)
        # return the resource config
        return cls(
            local=local_resource_config,
            gcp=gcp_resource_config,
            aws=aws_resource_config,
            azure=azure_resource_config,
        )

    def dump(self, resource_config_dir: str):
        # dump the local resource config
        local_resource_config_path = os.path.join(resource_config_dir, "local.json")
        self.local.dump(local_resource_config_path)
        # dump the gcp resource config
        gcp_resource_config_path = os.path.join(resource_config_dir, "gcp.json")
        self.gcp.dump(gcp_resource_config_path)
        # dump the aws resource config
        aws_resource_config_path = os.path.join(resource_config_dir, "aws.json")
        self.aws.dump(aws_resource_config_path)
        # dump the azure resource config
        azure_resource_config_path = os.path.join(resource_config_dir, "azure.json")
        self.azure.dump(azure_resource_config_path)
