from typing import Optional, Type

from buildflow.core.providers.provider import (
    PulumiProvider,
    SinkProvider,
    SourceProvider,
)
from buildflow.core.types.local_types import FileFormat, FilePath
from buildflow.core.io.local.strategies.file_strategies import FileSink


class FileProvider(SinkProvider, PulumiProvider):
    def __init__(
        self,
        *,
        file_path: FilePath,
        file_format: FileFormat,
        # source-only options
        # sink-only options
        # pulumi-only options
    ):
        self.file_path = file_path
        self.file_format = file_format
        # sink-only options
        # pulumi-only options

    def sink(self):
        return FileSink(
            file_path=self.file_path,
            file_format=self.file_format,
        )

    def pulumi_resources(self, type_: Optional[Type]):
        # Local file provider does not have any Pulumi resources
        return []
