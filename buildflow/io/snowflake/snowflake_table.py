import dataclasses
from typing import Optional, Union

from buildflow.core.io.primitive import Primitive, PrimitiveType
from buildflow.core.io.snowflake.providers.table_provider import SnowflakeTableProvider
from buildflow.core.providers.provider import (
    BackgroundTaskProvider,
    PulumiProvider,
    SinkProvider,
)
from buildflow.core.utils import uuid
from buildflow.io.aws.s3 import S3Bucket
from buildflow.io.gcp.storage import GCSBucket
from buildflow.types.portable import FileFormat


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
    flush_time_limit_secs: Optional[int] = 60

    # Optional arguments to configure pulumi. These can be set with the:
    # .options(...) method
    database_managed: bool = dataclasses.field(default=False, init=False)
    schema_managed: bool = dataclasses.field(default=False, init=False)
    snow_pipe_managed: bool = dataclasses.field(default=False, init=False)
    stage_managed: bool = dataclasses.field(default=False, init=False)

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
        managed: bool = False,
        database_managed: bool = True,
        schema_managed: bool = True,
    ) -> "SnowflakeTable":
        to_ret = super().options(managed)
        to_ret.database_managed = database_managed
        to_ret.schema_managed = schema_managed
        return to_ret

    def sink_provider(self) -> SinkProvider:
        return SnowflakeTableProvider(
            table=self.table,
            database=self.database,
            schema=self.schema,
            bucket_provider=self.bucket.sink_provider(),
            snow_pipe=self.snow_pipe,
            snowflake_stage=self.snowflake_stage,
            database_managed=self.database_managed,
            schema_managed=self.schema_managed,
            bucket_managed=self.bucket.managed,
            snow_pipe_managed=self.snow_pipe_managed,
            stage_managed=self.stage_managed,
            account=self.account,
            user=self.user,
            private_key=self.private_key,
            flush_time_secs=self.flush_time_limit_secs,
        )

    def pulumi_provider(self) -> PulumiProvider:
        return SnowflakeTableProvider(
            table=self.table,
            database=self.database,
            schema=self.schema,
            bucket_provider=self.bucket.pulumi_provider(),
            snow_pipe=self.snow_pipe,
            snowflake_stage=self.snowflake_stage,
            database_managed=self.database_managed,
            schema_managed=self.schema_managed,
            bucket_managed=self.bucket.managed,
            snow_pipe_managed=self.snow_pipe_managed,
            stage_managed=self.stage_managed,
            account=self.account,
            user=self.user,
            private_key=self.private_key,
            flush_time_secs=self.flush_time_limit_secs,
        )

    def background_task_provider(self) -> BackgroundTaskProvider:
        return SnowflakeTableProvider(
            table=self.table,
            database=self.database,
            schema=self.schema,
            bucket_provider=self.bucket.pulumi_provider(),
            snow_pipe=self.snow_pipe,
            snowflake_stage=self.snowflake_stage,
            database_managed=self.database_managed,
            schema_managed=self.schema_managed,
            bucket_managed=self.bucket.managed,
            snow_pipe_managed=self.snow_pipe_managed,
            stage_managed=self.stage_managed,
            account=self.account,
            user=self.user,
            private_key=self.private_key,
            flush_time_secs=self.flush_time_limit_secs,
        )
