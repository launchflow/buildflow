import dataclasses
from typing import List, Optional, Type, Union

import pulumi
import pulumi_snowflake

from buildflow.core.background_tasks.background_task import BackgroundTask
from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.core.credentials.gcp_credentials import GCPCredentials
from buildflow.io.aws.s3 import S3Bucket
from buildflow.io.gcp.storage import GCSBucket
from buildflow.io.provider import BackgroundTaskProvider, PulumiProvider, SinkProvider
from buildflow.io.snowflake.background_tasks.table_load_background_task import (
    SnowflakeUploadBackgroundTask,
)
from buildflow.io.snowflake.providers.schemas import type_to_snowflake_columns
from buildflow.io.snowflake.strategies.table_sink_startegy import SnowflakeTableSink
from buildflow.io.strategies.sink import SinkStrategy


class _SnowflakeTableSinkResource(pulumi.ComponentResource):
    def __init__(
        self,
        table: str,
        database: str,
        schema: str,
        bucket: Union[S3Bucket, GCSBucket],
        snow_pipe: Optional[str],
        snowflake_stage: Optional[str],
        database_managed: bool,
        schema_managed: bool,
        snow_pipe_managed: bool,
        stage_managed: bool,
        account: str,
        user: str,
        private_key: str,
        # pulumi_resource options (buildflow internal concept)
        type_: Optional[Type],
        credentials: AWSCredentials,
        opts: pulumi.ResourceOptions,
    ):
        super().__init__(
            "buildflow:snowflake:Table",
            f"buildflow-{database}-{schema}-{table}",
            None,
            opts,
        )

        outputs = {}

        table_id = f"{database}.{schema}.{table}"
        snowflake_provider = pulumi_snowflake.Provider(
            resource_name=f"{table_id}.snowflake_provider",
            account=account,
            username=user,
            private_key=private_key,
        )
        if type_ is None:
            raise ValueError(
                "Please specify an output type so we can determine the expected schema "
                "of the table."
            )
        if hasattr(type_, "__args__"):
            # Using a composite type hint like List or Optional
            type_ = type_.__args__[0]
        columns = type_to_snowflake_columns(type_)
        pulumi_snowflake_cols = []
        for column in columns:
            pulumi_snowflake_cols.append(
                pulumi_snowflake.TableColumnArgs(
                    name=column.name, type=column.col_type, nullable=column.nullable
                )
            )

        self.database_resource = None
        self.schema_resource = None
        running_depends = []
        if database_managed:
            self.database_resource = pulumi_snowflake.Database(
                database,
                opts=pulumi.ResourceOptions(parent=self, provider=snowflake_provider),
                name=database,
            )
            running_depends.append(self.database_resource)
            outputs["snowflake.database"] = self.database_resource.name
        if schema_managed:
            schema_id = f"{database}.{schema}"
            self.schema_resource = pulumi_snowflake.Schema(
                schema_id,
                opts=pulumi.ResourceOptions(
                    parent=self,
                    provider=snowflake_provider,
                    depends_on=running_depends,
                ),
                name=schema,
                database=database,
            )
            running_depends.append(self.schema_resource)
            outputs["snowflake.schema"] = schema_id

        self.table_resource = pulumi_snowflake.Table(
            table_id,
            columns=pulumi_snowflake_cols,
            database=database,
            schema=schema,
            name=table,
            opts=pulumi.ResourceOptions(
                parent=self, provider=snowflake_provider, depends_on=running_depends
            ),
        )
        outputs["snowflake.table"] = table_id

        self.stage_resource = None
        if stage_managed:
            snow_stage_id = f"{table_id}.{snowflake_stage}"
            stage_credentials = None
            if isinstance(credentials, AWSCredentials):
                stage_credentials = (
                    f"AWS_KEY_ID='{credentials.access_key_id}' "
                    f"AWS_SECRET_KEY='{credentials.secret_access_key}'"
                )
            self.stage_resource = pulumi_snowflake.Stage(
                snow_stage_id,
                opts=pulumi.ResourceOptions(
                    parent=self, provider=snowflake_provider, depends_on=running_depends
                ),
                name=snowflake_stage,
                database=database,
                schema=schema,
                copy_options="MATCH_BY_COLUMN_NAME = CASE_SENSITIVE",
                file_format="TYPE = PARQUET",
                url=bucket.bucket_url,
                credentials=stage_credentials,
            )
            outputs["snowflake.stage"] = snow_stage_id
            running_depends.append(self.stage_resource)
        self.snow_pipe_resource = None
        if snow_pipe_managed:
            copy_statement = (
                f'copy into "{database}"."{schema}"."{table}" '
                f'from @"{database}"."{schema}"."{snowflake_stage}";'
            )
            snow_pipe_id = f"{table_id}.{snow_pipe}"
            self.snow_pipe_resource = pulumi_snowflake.Pipe(
                snow_pipe_id,
                opts=pulumi.ResourceOptions(
                    parent=self, provider=snowflake_provider, depends_on=running_depends
                ),
                database=database,
                schema=schema,
                name=snow_pipe,
                copy_statement=copy_statement,
            )
            outputs["snowflake.pipe"] = snow_pipe_id


@dataclasses.dataclass
class SnowflakeTableProvider(SinkProvider, PulumiProvider, BackgroundTaskProvider):
    # Information about the table
    table: str
    database: str
    schema: str
    # Options for configuring flushing
    flush_time_secs: int
    # Bucket for staging data for upload to snowflake
    bucket: Union[S3Bucket, GCSBucket]
    snow_pipe: Optional[str]
    snowflake_stage: Optional[str]
    # Options for configuring pulumi
    database_managed: bool
    schema_managed: bool
    snow_pipe_managed: bool
    stage_managed: bool
    # Authentication information
    account: str
    user: str
    private_key: str

    def sink(self, credentials: Union[AWSCredentials, GCPCredentials]) -> SinkStrategy:
        return SnowflakeTableSink(
            credentials=credentials,
            bucket_sink=self.bucket.sink_provider().sink(credentials),
        )

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

    def pulumi_resource(
        self,
        type_: Optional[Type],
        credentials: Union[AWSCredentials, GCPCredentials],
        opts: pulumi.ResourceOptions,
    ) -> _SnowflakeTableSinkResource:
        return _SnowflakeTableSinkResource(
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
            type_=type_,
            credentials=credentials,
            opts=opts,
        )
