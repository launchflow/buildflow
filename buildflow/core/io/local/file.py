import dataclasses

from buildflow.config.cloud_provider_config import LocalOptions
from buildflow.core.io.primitive import LocalPrimtive
from buildflow.core.types.local_types import (
    FileFormat,
    FilePath,
)
from buildflow.core.io.local.providers.file_providers import FileProvider


@dataclasses.dataclass
class File(LocalPrimtive):
    file_path: FilePath
    file_format: FileFormat

    @classmethod
    def from_local_options(
        cls,
        local_options: LocalOptions,
        *,
        file_path: FilePath,
        file_format: FileFormat,
    ) -> "File":
        return cls(
            file_path=file_path,
            file_format=file_format,
        )

    def sink_provider(self) -> FileProvider:
        return FileProvider(
            file_path=self.file_path,
            file_format=self.file_format,
        )

    def pulumi_provider(self) -> FileProvider:
        return FileProvider(
            file_path=self.file_path,
            file_format=self.file_format,
        )
