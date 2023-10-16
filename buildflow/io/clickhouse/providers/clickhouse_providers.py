from buildflow.core.credentials import ClickhouseCredentials
from buildflow.core.types.clickhouse_types import ClickhouseDatabase, ClickhouseTable
from buildflow.io.clickhouse.strategies.clickhouse_strategies import ClickhouseSink
from buildflow.io.provider import SinkProvider


class ClickhouseProvider(SinkProvider):
    def __init__(
        self,
        *,
        database: ClickhouseDatabase,
        table: ClickhouseTable,
        # source-only options
        # sink-only options
        # pulumi-only options
    ):
        self.database = database
        self.table = table
        # sink-only options
        # pulumi-only options

    def sink(self, credentials: ClickhouseCredentials):
        return ClickhouseSink(
            credentials=credentials,
            database=self.database,
            table=self.table,
        )
