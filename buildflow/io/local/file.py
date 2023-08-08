import dataclasses
import os

from buildflow.config.cloud_provider_config import LocalOptions
from buildflow.core.types.shared_types import FilePath
from buildflow.io.local.providers.file_providers import FileProvider
from buildflow.io.primitive import LocalPrimtive
from buildflow.types.portable import FileFormat


@dataclasses.dataclass
class File(
    LocalPrimtive[
        # Pulumi provider type
        None,
        # Source provider type
        None,
        # Sink provider type
        FileProvider,
        # Background task provider type
        None,
    ]
):
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
