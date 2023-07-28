from typing import Optional, Type

from buildflow.core.credentials import EmptyCredentials
from buildflow.core.io.local.strategies.file_change_stream_strategies import (
    LocalFileChangeStreamSource,
)
from buildflow.core.providers.provider import PulumiProvider, SourceProvider
from buildflow.core.strategies.source import SourceStrategy
from buildflow.core.types.shared_types import FilePath
from buildflow.types.local import FileChangeStreamEventType


class LocalFileChangeStreamProvider(SourceProvider, PulumiProvider):
    def __init__(
        self,
        *,
        file_path: FilePath,
        event_types: Optional[
            FileChangeStreamEventType
        ] = FileChangeStreamEventType.CREATED,
    ):
        self.file_path = file_path
        self.event_types = event_types

    def source(self, credentials: EmptyCredentials) -> SourceStrategy:
        return LocalFileChangeStreamSource(
            credentials=credentials,
            file_path=self.file_path,
            event_types=self.event_types,
        )

    def pulumi_resources(self, type_: Optional[Type], depends_on: list = []):
        # Local file provider does not have any Pulumi resources
        return []
