import dataclasses
import os
from typing import Iterable

from buildflow.config.cloud_provider_config import LocalOptions
from buildflow.core.credentials.empty_credentials import EmptyCredentials
from buildflow.core.types.shared_types import FilePath
from buildflow.core.utils import uuid
from buildflow.io.local.strategies.file_change_stream_strategies import (
    LocalFileChangeStreamSource,
)
from buildflow.io.primitive import LocalPrimtive
from buildflow.types.local import FileChangeStreamEventType


@dataclasses.dataclass
class LocalFileChangeStream(LocalPrimtive):
    file_path: FilePath
    event_types: Iterable[FileChangeStreamEventType] = (
        FileChangeStreamEventType.CREATED,
    )

    def __post_init__(self):
        if not self.file_path.startswith("/"):
            self.file_path = os.path.join(os.getcwd(), self.file_path)

        self._primitive_id = uuid()

    def primitive_id(self):
        return self._primitive_id

    @classmethod
    def from_local_options(
        cls,
        local_options: LocalOptions,
        *,
        file_path: FilePath,
        event_types: Iterable[FileChangeStreamEventType] = (
            FileChangeStreamEventType.CREATED,
        ),
    ) -> "LocalPrimtive":
        """Create a primitive from LocalOptions."""
        return cls(file_path, event_types=event_types)

    def source(self, credentials: EmptyCredentials) -> LocalFileChangeStreamSource:
        return LocalFileChangeStreamSource(
            file_path=self.file_path,
            event_types=self.event_types,
            credentials=credentials,
        )
