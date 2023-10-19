import dataclasses
from typing import List, Optional, Type, Union

import pulumi

from buildflow.core.background_tasks.background_task import BackgroundTask
from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.core.credentials.gcp_credentials import GCPCredentials
from buildflow.core.utils import uuid
from buildflow.io.aws.s3 import S3Bucket
from buildflow.io.gcp.storage import GCSBucket
from buildflow.io.primitive import Primitive, PrimitiveType
from buildflow.io.snowflake.background_tasks.table_load_background_task import (
    SnowflakeUploadBackgroundTask,
)
from buildflow.io.snowflake.pulumi.snowflake_table_resource import (
    SnowflakeTableSinkResource,
)
from buildflow.io.snowflake.strategies.table_sink_startegy import SnowflakeTableSink
from buildflow.types.portable import FileFormat

_DEFAULT_DATABASE_MANAGED = True
_DEFAULT_SCHEMA_MANAGED = True
_DEFAULT_SNOW_PIPE_MANAGED = True
_DEFAULT_STAGE_MANAGED = True
_DEFAULT_FLUSH_TIME_LIMIT_SECS = 60


@dataclasses.dataclass
class SnowflakeTable(Primitive):
    # TODO: make these types more concrete
    # Required arguments
    table: str
    database: str
    schema: str
    bucket: Union[S3Bucket, GCSBucket]
    # Arguments for authentication
    account: Optional[str]
    user: Optional[str]
    private_key: Optional[str]
    # Optional arguments to configure sink
    # If snow pipe is provided, the sink will use snowpipe to load data
    # Otherwise you should run `buildflow apply` to have a snow pipe created
    # for you.
    snow_pipe: Optional[str] = None
    # If snowflake_stage is provided, the sink will use the provided stage
    # for copying data.
    # Otherwise you should run `buildflow apply` to have a stage created
    # for you.
    snowflake_stage: Optional[str] = None

    # Optional arguments to configure sink
    # The maximium number of seconds to wait before flushing to SnowPipe.
    flush_time_limit_secs: int = dataclasses.field(
        default=_DEFAULT_FLUSH_TIME_LIMIT_SECS, init=False
    )

    # Optional arguments to configure pulumi. These can be set with the:
    # .options(...) method
    database_managed: bool = dataclasses.field(
        default=_DEFAULT_DATABASE_MANAGED, init=False
    )
    schema_managed: bool = dataclasses.field(
        default=_DEFAULT_SCHEMA_MANAGED, init=False
    )
    snow_pipe_managed: bool = dataclasses.field(
        default=_DEFAULT_SNOW_PIPE_MANAGED, init=False
    )
    stage_managed: bool = dataclasses.field(default=_DEFAULT_STAGE_MANAGED, init=False)
    table_schema: Optional[Type] = dataclasses.field(default=None, init=False)

    def __post_init__(self):
        if isinstance(self.bucket, S3Bucket):
            self.primitive_type = PrimitiveType.AWS
        elif isinstance(self.bucket, GCSBucket):
            self.primitive_type = PrimitiveType.GCP
        else:
            raise ValueError(
                "Bucket must be of type S3Bucket or GCSBucket. Got: "
                f"{type(self.bucket)}"
            )
        self.bucket.file_format = FileFormat.PARQUET
        self.bucket.file_path = f"{uuid()}.parquet"
        self.snow_pipe_managed = self.snow_pipe is None
        if self.snow_pipe is None:
            self.snow_pipe = "buildflow_managed_snow_pipe"
        self.stage_managed = self.snowflake_stage is None
        if self.snowflake_stage is None:
            self.snowflake_stage = "buildflow_managed_snowflake_stage"

    def options(
        self,
        # Pulumi management options
        database_managed: bool = _DEFAULT_DATABASE_MANAGED,
        schema_managed: bool = _DEFAULT_SCHEMA_MANAGED,
        table_schema: Optional[Type] = None,
        # Sink options
        flush_time_limit_secs: int = _DEFAULT_FLUSH_TIME_LIMIT_SECS,
    ) -> "SnowflakeTable":
        self.database_managed = database_managed
        self.schema_managed = schema_managed
        self.table_schema = table_schema
        self.flush_time_limit_secs = flush_time_limit_secs
        return self

    def background_tasks(
        self, credentials: Union[AWSCredentials, GCPCredentials]
    ) -> List[BackgroundTask]:
        return [
            SnowflakeUploadBackgroundTask(
                credentials=credentials,
                bucket_name=self.bucket.bucket_name,
                account=self.account,
                user=self.user,
                database=self.database,
                schema=self.schema,
                private_key=self.private_key,
                pipe=self.snow_pipe,
                flush_time_secs=self.flush_time_secs,
            )
        ]

    def primitive_id(self) -> str:
        return f"{self.database}.{self.schema}.{self.table}"

    def pulumi_resources(
        self,
        credentials: Union[AWSCredentials, GCPCredentials],
        opts: pulumi.ResourceOptions,
    ) -> List[pulumi.Resource]:
        return [
            SnowflakeTableSinkResource(
                table=self.table,
                database=self.database,
                schema=self.schema,
                bucket=self.bucket,
                snowflake_stage=self.snowflake_stage,
                snow_pipe=self.snow_pipe,
                database_managed=self.database_managed,
                schema_managed=self.schema_managed,
                snow_pipe_managed=self.snow_pipe_managed,
                stage_managed=self.stage_managed,
                account=self.account,
                user=self.user,
                private_key=self.private_key,
                table_schema=self.table_schema,
                credentials=credentials,
                opts=opts,
            )
        ]

    def sink(
        self, credentials: Union[AWSCredentials, GCPCredentials]
    ) -> SnowflakeTableSink:
        return SnowflakeTableSink(
            credentials=credentials,
            bucket_sink=self.bucket.sink(credentials),
        )
