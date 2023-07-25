import dataclasses

from buildflow.config.cloud_provider_config import LocalOptions
from buildflow.core.io.primitive import LocalPrimtive
from buildflow.core.io.local.providers.file_change_stream_provider import (
    LocalFileChangeStreamProvider,
)
from buildflow.core.types.local_types import FilePath


@dataclasses.dataclass
class LocalFileChangeStream(LocalPrimtive):
    file_path: FilePath

    @classmethod
    def from_local_options(cls, local_options: LocalOptions) -> "LocalPrimtive":
        """Create a primitive from LocalOptions."""
        raise NotImplementedError(
            "LocalPrimtive.from_local_options() is not implemented."
        )

    def source_provider(self) -> LocalFileChangeStreamProvider:
        return LocalFileChangeStreamProvider(
            file_path=self.file_path,
        )

    def pulumi_provider(self) -> LocalFileChangeStreamProvider:
        return LocalFileChangeStreamProvider(
            file_path=self.file_path,
        )
