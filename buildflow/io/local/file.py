import dataclasses
import os

from buildflow.config.cloud_provider_config import LocalOptions
from buildflow.core.credentials.empty_credentials import EmptyCredentials
from buildflow.core.types.shared_types import FilePath
from buildflow.core.utils import uuid
from buildflow.io.local.strategies.file_strategies import FileSink
from buildflow.io.primitive import LocalPrimtive
from buildflow.io.strategies.sink import SinkStrategy
from buildflow.types.portable import FileFormat


@dataclasses.dataclass
class File(LocalPrimtive):
    file_path: FilePath
    file_format: FileFormat

    def __post_init__(self):
        if not self.file_path.startswith("/"):
            self.file_path = os.path.join(os.getcwd(), self.file_path)
        if isinstance(self.file_format, str):
            self.file_format = FileFormat(self.file_format)

        self._primitive_id = uuid()

    def primitive_id(self):
        return self._primitive_id

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

    def sink(self, credentials: EmptyCredentials) -> SinkStrategy:
        return FileSink(
            file_path=self.file_path,
            file_format=self.file_format,
            credentials=credentials,
        )
