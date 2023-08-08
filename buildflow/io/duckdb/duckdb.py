import dataclasses
import os

from buildflow.config.cloud_provider_config import LocalOptions
from buildflow.core.types.duckdb_types import DuckDBDatabase, DuckDBTableID
from buildflow.io.duckdb.providers.duckdb_providers import DuckDBProvider
from buildflow.io.primitive import LocalPrimtive


@dataclasses.dataclass
class DuckDBTable(
    LocalPrimtive[
        # Pulumi provider type
        None,
        # Source provider type
        None,
        # Sink provider type
        DuckDBProvider,
        # Background task provider type
        None,
    ]
):
    database: DuckDBDatabase
    table: DuckDBTableID

    def __post_init__(self):
        # TODO: need to update this to work with motherduck not just local db
        if not self.database.startswith("/"):
            self.database = os.path.join(os.getcwd(), self.database)

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

    def _pulumi_provider(self) -> DuckDBProvider:
        return DuckDBProvider(database=self.database, table=self.table)
