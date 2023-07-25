import dataclasses
import os

from buildflow.config.cloud_provider_config import LocalOptions
from buildflow.core.io.local.providers.file_providers import FileProvider
from buildflow.core.io.primitive import LocalPrimtive
from buildflow.core.types.local_types import FileFormat
from buildflow.core.types.shared_types import FilePath


@dataclasses.dataclass
class File(LocalPrimtive):
    file_path: FilePath
    file_format: FileFormat

    def __post_init__(self):
        if not self.file_path.startswith("/"):
            self.file_path = os.path.join(os.getcwd(), self.file_path)
        if isinstance(self.file_format, str):
            self.file_format = FileFormat(self.file_format)

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

    def sink_provider(
        self,
    ) -> FileProvider:
        return FileProvider(
            file_path=self.file_path,
            file_format=self.file_format,
        )

    def pulumi_provider(self) -> FileProvider:
        return FileProvider(
            file_path=self.file_path,
            file_format=self.file_format,
        )
