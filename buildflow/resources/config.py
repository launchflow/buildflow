import dataclasses
import os

from google.cloud.client import ClientWithProject

from buildflow.utils import CONFIG_DIR


@dataclasses.dataclass
class LocalResourceConfig:
    output_dir: str

    @classmethod
    def DEFAULT(cls) -> "LocalResourceConfig":
        output_dir = os.path.join(CONFIG_DIR, "local")
        return cls(output_dir=output_dir)


@dataclasses.dataclass
class GCPResourceConfig:
    default_project_id: str
    default_region: str
    default_zone: str

    @classmethod
    def DEFAULT(cls) -> "GCPResourceConfig":
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


@dataclasses.dataclass
class AWSResourceConfig:
    default_region: str

    @classmethod
    def DEFAULT(cls) -> "AWSResourceConfig":
        # TODO: make these configurable
        return cls(default_region="us-east-1")


@dataclasses.dataclass
class AzureResourceConfig:
    default_region: str

    @classmethod
    def DEFAULT(cls) -> "AzureResourceConfig":
        # TODO: make these configurable
        return cls(default_region="eastus")


@dataclasses.dataclass
class ResourceConfig:
    local: LocalResourceConfig
    gcp: GCPResourceConfig
    aws: AWSResourceConfig
    azure: AzureResourceConfig

    @classmethod
    def DEFAULT(cls) -> "ResourceConfig":
        return cls(
            local=LocalResourceConfig.DEFAULT(),
            gcp=GCPResourceConfig.DEFAULT(),
            aws=AWSResourceConfig.DEFAULT(),
            azure=AzureResourceConfig.DEFAULT(),
        )
