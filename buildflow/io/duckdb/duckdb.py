import dataclasses
import logging
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
        if not self.database.startswith("md:") and not self.database.startswith("/"):
            self.database = os.path.join(os.getcwd(), self.database)
        if self.database.startswith("md:") and "MOTHERDUCK_TOKEN" not in os.environ:
            logging.warning(
                "MOTHERDUCK_TOKEN not found in environment variables, "
                "but you are writing to a database starting with `md:`."
            )

    def primitive_id(self):
        db = self.database
        if "?" in self.database:
            db = self.database.split("?")[0]
        return f"{db}:{self.table}"

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
