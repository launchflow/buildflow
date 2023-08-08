from typing import Optional, Type

from buildflow.core.credentials import EmptyCredentials
from buildflow.core.types.duckdb_types import DuckDBDatabase, DuckDBTableID
from buildflow.io.duckdb.strategies.duckdb_strategies import DuckDBSink
from buildflow.io.provider import PulumiProvider, SinkProvider


class DuckDBProvider(SinkProvider, PulumiProvider):
    def __init__(
        self,
        *,
        database: DuckDBDatabase,
        table: DuckDBTableID,
        # source-only options
        # sink-only options
        # pulumi-only options
    ):
        self.database = database
        self.table = table
        # sink-only options
        # pulumi-only options

    def sink(self, credentials: EmptyCredentials):
        return DuckDBSink(
            credentials=credentials,
            database=self.database,
            table=self.table,
        )

    def pulumi_resource(
        self,
        type_: Optional[Type],
        credeitnals: EmptyCredentials,
        depends_on: list = [],
    ):
        # Local file provider does not have any Pulumi resources
        return []
