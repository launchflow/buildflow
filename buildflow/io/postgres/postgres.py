import dataclasses
from typing import Optional, Union

from buildflow.core.utils import uuid
from buildflow.io.aws.s3 import S3Bucket
from buildflow.io.gcp.storage import GCSBucket
from buildflow.io.primitive import Primitive, PrimitiveType
from buildflow.io.provider import BackgroundTaskProvider, PulumiProvider, SinkProvider
from buildflow.io.snowflake.providers.table_provider import SnowflakeTableProvider
from buildflow.types.portable import FileFormat

_DEFAULT_DATABASE_MANAGED = True
_DEFAULT_SCHEMA_MANAGED = True
_DEFAULT_SNOW_PIPE_MANAGED = True
_DEFAULT_STAGE_MANAGED = True
_DEFAULT_FLUSH_TIME_LIMIT_SECS = 60


@dataclasses.dataclass
class SnowflakeTable(
    Primitive[
        # Pulumi provider type
        SnowflakeTableProvider,
        # Source provider type
        None,
        # Sink provider type
        SnowflakeTableProvider,
        # Background task provider type
        SnowflakeTableProvider,
    ]
):
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
        managed: bool = False,
        database_managed: bool = _DEFAULT_DATABASE_MANAGED,
        schema_managed: bool = _DEFAULT_SCHEMA_MANAGED,
        # Sink options
        flush_time_limit_secs: int = _DEFAULT_FLUSH_TIME_LIMIT_SECS,
    ) -> "SnowflakeTable":
        to_ret = super().options(managed)
        to_ret.database_managed = database_managed
        to_ret.schema_managed = schema_managed
        to_ret.flush_time_limit_secs = flush_time_limit_secs
        return to_ret

    def sink_provider(self) -> SinkProvider:
        return SnowflakeTableProvider(
            table=self.table,
            database=self.database,
            schema=self.schema,
            bucket=self.bucket,
            snow_pipe=self.snow_pipe,
            snowflake_stage=self.snowflake_stage,
            database_managed=self.database_managed,
            schema_managed=self.schema_managed,
            snow_pipe_managed=self.snow_pipe_managed,
            stage_managed=self.stage_managed,
            account=self.account,
            user=self.user,
            private_key=self.private_key,
            flush_time_secs=self.flush_time_limit_secs,
        )

    def _pulumi_provider(self) -> PulumiProvider:
        return SnowflakeTableProvider(
            table=self.table,
            database=self.database,
            schema=self.schema,
            bucket=self.bucket,
            snow_pipe=self.snow_pipe,
            snowflake_stage=self.snowflake_stage,
            database_managed=self.database_managed,
            schema_managed=self.schema_managed,
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
            bucket=self.bucket._pulumi_provider(),
            snow_pipe=self.snow_pipe,
            snowflake_stage=self.snowflake_stage,
            database_managed=self.database_managed,
            schema_managed=self.schema_managed,
            snow_pipe_managed=self.snow_pipe_managed,
            stage_managed=self.stage_managed,
            account=self.account,
            user=self.user,
            private_key=self.private_key,
            flush_time_secs=self.flush_time_limit_secs,
        )
