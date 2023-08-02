from buildflow.core.credentials import EmptyCredentials
from buildflow.core.io.local.strategies.file_strategies import FileSink
from buildflow.core.providers.provider import EmptyPulumiProvider, SinkProvider
from buildflow.core.types.shared_types import FilePath
from buildflow.types.local import FileFormat


class FileProvider(SinkProvider, EmptyPulumiProvider):
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

    def sink(self, credentials: EmptyCredentials):
        return FileSink(
            credentials=credentials,
            file_path=self.file_path,
            file_format=self.file_format,
        )
        # Local file provider does not have any Pulumi resources
        return []
