"""Allows listening to file changes."""
import dataclasses
from typing import Iterable

from buildflow.config.cloud_provider_config import CloudProvider, CloudProviderConfig
from buildflow.core.types.shared_types import FilePath
from buildflow.io.gcp.gcs_file_change_stream import GCSFileChangeStream
from buildflow.io.local.file_change_stream import LocalFileChangeStream
from buildflow.io.primitive import PortablePrimtive, Primitive
from buildflow.io.strategies._strategy import StategyType
from buildflow.types.gcp import GCSChangeStreamEventType
from buildflow.types.local import FileChangeStreamEventType, PortableFileChangeEventType


@dataclasses.dataclass
class FileChangeStream(PortablePrimtive):
    file_path: FilePath
    event_types: Iterable[PortableFileChangeEventType] = (
        PortableFileChangeEventType.CREATED,
    )

    def to_cloud_primitive(
        self, cloud_provider_config: CloudProviderConfig, strategy_type: StategyType
    ) -> Primitive:
        if strategy_type != StategyType.SOURCE:
            raise ValueError(
                f"Unsupported strategy type for FileStream: {strategy_type}"
            )
        # GCP Implementations
        if cloud_provider_config.default_cloud_provider == CloudProvider.GCP:
            event_types = [
                GCSChangeStreamEventType.from_portable_type(et)
                for et in self.event_types
            ]
            return GCSFileChangeStream.from_gcp_options(
                gcp_options=cloud_provider_config.gcp_options,
                bucket_name=self.file_path,
                event_types=event_types,
            )
        # AWS Implementations
        elif cloud_provider_config.default_cloud_provider == CloudProvider.AWS:
            raise NotImplementedError("AWS is not implemented for FileStream.")
        # Azure Implementations
        elif cloud_provider_config.default_cloud_provider == CloudProvider.AZURE:
            raise NotImplementedError("Azure is not implemented for FileStream.")
        # Local Implementations
        elif cloud_provider_config.default_cloud_provider == CloudProvider.LOCAL:
            event_types = [
                FileChangeStreamEventType.from_portable_type(et)
                for et in self.event_types
            ]
            return LocalFileChangeStream.from_local_options(
                local_options=cloud_provider_config.local_options,
                file_path=self.file_path,
                event_types=event_types,
            )
        # Sanity check
        else:
            raise ValueError(
                f"Unknown resource provider: {cloud_provider_config.default_cloud_provider}"  # noqa: E501
            )
