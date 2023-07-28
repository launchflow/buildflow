import dataclasses

from buildflow.config.cloud_provider_config import LocalOptions
from buildflow.core.io.local.providers.duckdb_providers import DuckDBProvider
from buildflow.core.io.primitive import LocalPrimtive
from buildflow.core.types.local_types import DuckDBDatabase, DuckDBTable


@dataclasses.dataclass
class DuckDB(LocalPrimtive):
    database: DuckDBDatabase
    table: DuckDBTable

    @classmethod
    def from_local_options(
        cls,
        local_options: LocalOptions,
        *,
        database: DuckDBDatabase,
        table: DuckDBTable,
    ) -> "LocalPrimtive":
        """Create a primitive from LocalOptions."""
        return cls(database, table)

    def sink_provider(self) -> DuckDBProvider:
        return DuckDBProvider(database=self.database, table=self.table)

    def pulumi_provider(self) -> DuckDBProvider:
        return DuckDBProvider(database=self.database, table=self.table)
