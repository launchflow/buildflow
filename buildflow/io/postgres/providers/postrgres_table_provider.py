from typing import Optional, Type

import pulumi

from buildflow.core.credentials.gcp_credentials import GCPCredentials
from buildflow.io.gcp.cloud_sql_database import CloudSQLDatabase
from buildflow.io.postgres.startegies.postgres_table_stragies import PostgresTableSink
from buildflow.io.provider import PulumiProvider, SinkProvider
from buildflow.io.strategies.sink import SinkStrategy


class _PostgresTableResource(pulumi.ComponentResource):
    def __init__(
        self,
        table: str,
        database: CloudSQLDatabase,
        # pulumi_resource options (buildflow internal concept)
        type_: Optional[Type],
        credentials: GCPCredentials,
        opts: pulumi.ResourceOptions,
    ):
        super().__init__(
            "buildflow:postgres:Table",
            f"buildflow-{database.instance.project_id}-{database.instance.instance_name}-{database.database_name}-{table}",
            None,
            opts,
        )
        opts = pulumi.ResourceOptions.merge(opts, pulumi.ResourceOptions(parent=self))

        self.database_resource = None
        database_pulumi_provider = database.pulumi_provider()
        if database_pulumi_provider is not None:
            self.database_resource = database_pulumi_provider.pulumi_resource(
                type_, credentials, opts
            )
            opts = pulumi.ResourceOptions.merge(
                opts, pulumi.ResourceOptions(depends_on=self.instance_resource)
            )

        self.register_outputs()


class PostgresTableProvider(PulumiProvider, SinkProvider):
    def __init__(self, table: str, database: CloudSQLDatabase):
        self.table = table
        self.database = database

    def sink(self, credentials: GCPCredentials) -> SinkStrategy:
        return PostgresTableSink(credentials)

    def pulumi_resource(
        self,
        type_: Optional[Type],
        credentials: GCPCredentials,
        opts: pulumi.ResourceOptions,
    ):
        return _PostgresTableResource(
            self.table, self.database, type_, credentials, opts
        )
