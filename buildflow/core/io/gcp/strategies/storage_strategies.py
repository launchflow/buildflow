import os

from buildflow.core.credentials import GCPCredentials
from buildflow.core.io.local.strategies.file_strategies import FileSink
from buildflow.core.io.utils.file_systems import get_file_system
from buildflow.core.types.gcp_types import GCPProjectID, GCSBucketName
from buildflow.core.types.shared_types import FilePath
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
        self.credentials = credentials
        self.strategy_id = "gcs-bucket-sink"
        self.bucket_name = bucket_name
        self.file_path = os.path.join(self.bucket_name, file_path)
        self.file_format = file_format
        self.file_system = get_file_system(credentials)
        self.project_id = project_id
