import os

from buildflow.core.credentials import GCPCredentials
from buildflow.core.types.gcp_types import GCPProjectID, GCSBucketName
from buildflow.core.types.shared_types import FilePath
from buildflow.io.local.strategies.file_strategies import FileSink
from buildflow.io.utils.file_systems import get_file_system
from buildflow.types.portable import FileFormat


class GCSBucketSink(FileSink):
    def __init__(
        self,
        *,
        credentials: GCPCredentials,
        project_id: GCPProjectID,
        bucket_name: GCSBucketName,
        file_path: FilePath,
        file_format: FileFormat,
    ):
        super().__init__(
            credentials=credentials,
            file_path=os.path.join(bucket_name, file_path),
            file_format=file_format,
            file_system=get_file_system(credentials),
        )
        self.strategy_id = "gcs-bucket-sink"
        self.bucket_name = bucket_name
        self.project_id = project_id
