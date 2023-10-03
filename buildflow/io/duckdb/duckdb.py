import dataclasses
import os

from buildflow.config.cloud_provider_config import LocalOptions
from buildflow.core.credentials.empty_credentials import EmptyCredentials
from buildflow.core.types.duckdb_types import DuckDBDatabase, DuckDBTableID
from buildflow.io.duckdb.strategies.duckdb_strategies import DuckDBSink
from buildflow.io.primitive import LocalPrimtive


@dataclasses.dataclass
class DuckDBTable(LocalPrimtive):
    database: DuckDBDatabase
    table: DuckDBTableID

    def __post_init__(self):
        if not self.database.startswith("/"):
            self.database = os.path.join(os.getcwd(), self.database)

    def primitive_id(self):
        return f"{self.database}::{self.table}"

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

    def sink(self, credentials: EmptyCredentials):
        return DuckDBSink(
            credentials=credentials,
            database=self.database,
            table=self.table,
        )
