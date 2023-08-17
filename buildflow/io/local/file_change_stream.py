import dataclasses
import os
from typing import Iterable

from buildflow.config.cloud_provider_config import LocalOptions
from buildflow.core.types.shared_types import FilePath
from buildflow.io.local.providers.file_change_stream_provider import (
    LocalFileChangeStreamProvider,
)
from buildflow.io.primitive import LocalPrimtive
from buildflow.types.local import FileChangeStreamEventType


@dataclasses.dataclass
class LocalFileChangeStream(
    LocalPrimtive[
        # Pulumi provider type
        None,
        # Source provider type
        LocalFileChangeStreamProvider,
        # Sink provider type
        None,
        # Background task provider type
        None,
    ]
):
    file_path: FilePath
    event_types: Iterable[FileChangeStreamEventType] = (
        FileChangeStreamEventType.CREATED,
    )

    def __post_init__(self):
        if not self.file_path.startswith("/"):
            self.file_path = os.path.join(os.getcwd(), self.file_path)

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

    def source_provider(self) -> LocalFileChangeStreamProvider:
        return LocalFileChangeStreamProvider(
            file_path=self.file_path, event_types=self.event_types
        )

    def _pulumi_provider(self) -> LocalFileChangeStreamProvider:
        return LocalFileChangeStreamProvider(
            file_path=self.file_path, event_types=self.event_types
        )
