import dataclasses
from typing import List, Optional, Type, Union

import pulumi
import pulumi_snowflake

from buildflow.core.background_tasks.background_task import BackgroundTask
from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.core.credentials.gcp_credentials import GCPCredentials
from buildflow.core.io.aws.providers.s3_provider import S3BucketProvider
from buildflow.core.io.gcp.providers.storage_providers import GCSBucketProvider
from buildflow.core.io.snowflake.background_tasks.table_load_background_task import (
    SnowflakeUploadBackgroundTask,
)
from buildflow.core.io.snowflake.providers.schemas import type_to_snowflake_columns
from buildflow.core.io.snowflake.strategies.table_sink_startegy import (
    SnowflakeTableSink,
)
from buildflow.core.providers.provider import (
    BackgroundTaskProvider,
    PulumiProvider,
    SinkProvider,
)
from buildflow.core.resources.pulumi import PulumiResource
from buildflow.core.strategies.sink import SinkStrategy


@dataclasses.dataclass
class SnowflakeTableProvider(SinkProvider, PulumiProvider, BackgroundTaskProvider):
    # Information about the table
    table: str
    database: str
    schema: str
    # Options for configuring flushing
    flush_time_secs: int
    # Bucket for staging data for upload to snowflake
    bucket_provider: Union[S3BucketProvider, GCSBucketProvider]
    snow_pipe: Optional[str]
    snowflake_stage: Optional[str]
    # Options for configuring pulumi
    database_managed: bool
    schema_managed: bool
    bucket_managed: bool
    snow_pipe_managed: bool
    stage_managed: bool
    # Authentication information
    account: str
    user: str
    private_key: str

    def sink(self, credentials: Union[AWSCredentials, GCPCredentials]) -> SinkStrategy:
        return SnowflakeTableSink(
            credentials=credentials,
            bucket_sink=self.bucket_provider.sink(credentials),
        )

    def background_tasks(
        self, credentials: Union[AWSCredentials, GCPCredentials]
    ) -> List[BackgroundTask]:
        return [
            SnowflakeUploadBackgroundTask(
                credentials=credentials,
                bucket_name=self.bucket_provider.bucket_name,
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
        depends_on: List[PulumiResource] = [],
    ) -> List[PulumiResource]:
        resources = []
        if self.bucket_managed:
            bucket_resources = self.bucket_provider.pulumi_resources(
                type_=type_, credentials=credentials, depends_on=depends_on
            )
            resources.extend(bucket_resources)

        table_id = f"{self.database}.{self.schema}.{self.table}"

        snowflake_provider = pulumi_snowflake.Provider(
            resource_name=f"{table_id}.snowflake_provider",
            account=self.account,
            username=self.user,
            private_key=self.private_key,
        )
        if type_ is None:
            raise ValueError(
                "Please specify an output type so we can determine the expected schema "
                "of the table."
            )
        depends = [tr.resource for tr in depends_on]
        if self.database_managed:
            database_resource = pulumi_snowflake.Database(
                self.database,
                opts=pulumi.ResourceOptions(
                    provider=snowflake_provider, depends_on=depends
                ),
                name=self.database,
            )
            depends.append(database_resource)
            pulumi.export("snowflake.database.name", self.database)
            resources.append(
                PulumiResource(
                    resource_id=self.database,
                    resource=database_resource,
                    exports={"snowflake.database.name": self.database},
                )
            )
        if self.schema_managed:
            schema_id = f"{self.database}.{self.schema}"
            schema_resource = pulumi_snowflake.Schema(
                schema_id,
                database=self.database,
                opts=pulumi.ResourceOptions(
                    provider=snowflake_provider, depends_on=depends
                ),
                name=self.schema,
            )
            depends.append(schema_resource)
            pulumi.export("snowflake.schema.id", schema_id)
            resources.append(
                PulumiResource(
                    resource_id=schema_id,
                    resource=schema_resource,
                    exports={"snowflake.schema.id": self.database},
                )
            )
        columns = None
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

        table_resource = pulumi_snowflake.Table(
            table_id,
            columns=pulumi_snowflake_cols,
            database=self.database,
            schema=self.schema,
            name=self.table,
            opts=pulumi.ResourceOptions(
                provider=snowflake_provider, depends_on=depends
            ),
        )
        pulumi.export("snowflake.table.id", table_id)
        resources.append(
            PulumiResource(
                resource_id=table_id,
                resource=table_resource,
                exports={"snowflake.table.i": table_id},
            )
        )
        if self.stage_managed:
            snow_stage_id = f"{table_id}.{self.snowflake_stage}"
            stage_credentials = None
            if isinstance(credentials, AWSCredentials):
                stage_credentials = (
                    f"AWS_KEY_ID='{credentials.access_key_id}' "
                    f"AWS_SECRET_KEY='{credentials.secret_access_key}'"
                )
            snow_stage = pulumi_snowflake.Stage(
                f"{table_id}.buildflow_snowflake_stage",
                opts=pulumi.ResourceOptions(
                    provider=snowflake_provider, depends_on=depends
                ),
                name=self.snowflake_stage,
                database=self.database,
                schema=self.schema,
                copy_options="MATCH_BY_COLUMN_NAME = CASE_SENSITIVE",
                file_format="TYPE = PARQUET",
                url=self.bucket_provider.bucket_url,
                credentials=stage_credentials,
            )
            depends.append(snow_stage)
            pulumi.export(f"snowflake.stage.{snow_stage_id}", snow_stage_id)
            resources.append(
                PulumiResource(
                    resource_id=snow_stage_id,
                    resource=snow_stage,
                    exports={f"snowflake.stage.{snow_stage_id}": snow_stage_id},
                )
            )
        if self.snow_pipe_managed:
            copy_statement = (
                f'copy into "{self.database}"."{self.schema}"."{self.table}" '
                f'from @"{self.database}"."{self.schema}"."{self.snowflake_stage}";'
            )
            snow_pipe_id = f"{table_id}.{self.snow_pipe}"
            snow_pipe = pulumi_snowflake.Pipe(
                snow_pipe_id,
                opts=pulumi.ResourceOptions(
                    provider=snowflake_provider, depends_on=depends
                ),
                database=self.database,
                schema=self.schema,
                name=self.snow_pipe,
                copy_statement=copy_statement,
            )
            depends.append(snow_pipe)
            pulumi.export(f"snowflake.pipe.{snow_pipe_id}", snow_pipe_id)
            resources.append(
                PulumiResource(
                    resource_id=snow_pipe_id,
                    resource=snow_pipe,
                    exports={f"snowflake.pipe.{snow_pipe_id}": snow_pipe_id},
                )
            )
        return resources
