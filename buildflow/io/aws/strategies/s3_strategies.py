import os

from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.core.types.aws_types import S3BucketName
from buildflow.core.types.shared_types import FilePath
from buildflow.io.local.strategies.file_strategies import FileSink
from buildflow.io.utils.file_systems import get_file_system
from buildflow.types.portable import FileFormat


class S3BucketSink(FileSink):
    def __init__(
        self,
        credentials: AWSCredentials,
        bucket_name: S3BucketName,
        file_path: FilePath,
        file_format: FileFormat,
    ):
        super().__init__(
            credentials=credentials,
            file_path=os.path.join(bucket_name, file_path),
            file_format=file_format,
            file_system=get_file_system(credentials),
        )
        self.strategy_id = "s3-bucket-sink"
        self.bucket_name = bucket_name
