import enum
import os
from typing import Any, Callable, Dict, Type, Union

from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.core.credentials.gcp_credentials import GCPCredentials
from buildflow.core.utils import uuid
from buildflow.io.aws.strategies.s3_strategies import S3BucketSink
from buildflow.io.gcp.strategies.storage_strategies import GCSBucketSink
from buildflow.io.strategies.sink import Batch, SinkStrategy


class _BucketType(enum.Enum):
    S3 = 1
    GCS = 2


_BASE_STAGING_DIR = "buildflow-staging"


class SnowflakeTableSink(SinkStrategy):
    def __init__(
        self,
        credentials: Union[AWSCredentials, GCPCredentials],
        # Bucket for staging data for upload to snowflake
        bucket_sink: Union[S3BucketSink, GCSBucketSink],
    ):
        super().__init__(credentials, "snowflake-table-sink")
        self.credentials = credentials
        self.bucket_sink = bucket_sink
        self.bucket_sink.file_path = self._get_new_file_path()

    def _get_new_file_path(self) -> str:
        return os.path.join(
            self.bucket_sink.bucket_name, _BASE_STAGING_DIR, f"{uuid()}.parquet"
        )

    def push_converter(
        self, user_defined_type: Type
    ) -> Callable[[Any], Dict[str, Any]]:
        return self.bucket_sink.push_converter(user_defined_type)

    async def push(self, batch: Batch):
        await self.bucket_sink.push(batch)
        self.bucket_sink.file_path = self._get_new_file_path()
