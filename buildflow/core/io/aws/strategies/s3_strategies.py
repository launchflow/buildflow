import os

from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.core.io.local.strategies.file_strategies import FileSink
from buildflow.core.io.utils.file_systems import get_file_system
from buildflow.core.types.aws_types import S3BucketName
from buildflow.core.types.shared_types import FilePath
from buildflow.types.portable import FileFormat


class S3BucketSink(FileSink):
    def __init__(
        self,
        credentials: AWSCredentials,
        bucket_name: S3BucketName,
        file_path: FilePath,
        file_format: FileFormat,
    ):
        self.credentials = credentials
        self.strategy_id = "s3-bucket-sink"
        self.bucket_name = bucket_name
        self.file_path = os.path.join(self.bucket_name, file_path)
        self.file_format = file_format
        self.file_system = get_file_system(credentials)
