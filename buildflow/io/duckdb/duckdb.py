import dataclasses

from buildflow.config.cloud_provider_config import LocalOptions
from buildflow.core.io.duckdb.providers.duckdb_providers import DuckDBProvider
from buildflow.core.io.primitive import LocalPrimtive
from buildflow.core.types.duckdb_types import DuckDBDatabase, DuckDBTableID


@dataclasses.dataclass
class DuckDBTable(LocalPrimtive):
    database: DuckDBDatabase
    table: DuckDBTableID

    @classmethod
    def from_local_options(
        cls,
        local_options: LocalOptions,
        *,
        database: DuckDBDatabase,
        table: DuckDBTableID,
    ) -> "LocalPrimtive":
        """Create a primitive from LocalOptions."""
        return cls(database, table)

    def sink_provider(self) -> DuckDBProvider:
        return DuckDBProvider(database=self.database, table=self.table)

    def pulumi_provider(self) -> DuckDBProvider:
        return DuckDBProvider(database=self.database, table=self.table)
