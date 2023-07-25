import dataclasses
import os
from typing import Iterable

from buildflow.config.cloud_provider_config import LocalOptions
from buildflow.core.io.primitive import LocalPrimtive
from buildflow.core.io.local.providers.file_change_stream_provider import (
    LocalFileChangeStreamProvider,
)
from buildflow.core.types.local_types import FileChangeStreamEvents
from buildflow.core.types.shared_types import FilePath


@dataclasses.dataclass
class LocalFileChangeStream(LocalPrimtive):
    file_path: FilePath
    event_types: Iterable[FileChangeStreamEvents]

    def __post_init__(self):
        if not self.file_path.startswith("/"):
            self.file_path = os.path.join(os.getcwd(), self.file_path)

    @classmethod
    def from_local_options(
        cls,
        local_options: LocalOptions,
        *,
        file_path: FilePath,
        event_types: Iterable[FileChangeStreamEvents] = (
            FileChangeStreamEvents.CREATED,
        ),
    ) -> "LocalPrimtive":
        """Create a primitive from LocalOptions."""
        return cls(file_path, event_types=event_types)

    def source_provider(self) -> LocalFileChangeStreamProvider:
        return LocalFileChangeStreamProvider(
            file_path=self.file_path, event_types=self.event_types
        )

    def pulumi_provider(self) -> LocalFileChangeStreamProvider:
        return LocalFileChangeStreamProvider(
            file_path=self.file_path, event_types=self.event_types
        )
