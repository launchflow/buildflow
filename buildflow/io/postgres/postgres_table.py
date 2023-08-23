import dataclasses

from buildflow.io.gcp.cloud_sql_database import CloudSQLDatabase
from buildflow.io.postgres.providers.postrgres_table_provider import (
    PostgresTableProvider,
)
from buildflow.io.primitive import Primitive, PrimitiveType


@dataclasses.dataclass
class PostgresTable(Primitive):
    # TODO: make these types more concrete
    table: str
    # TODO update this so it can take in an AWS managed postgres database also
    database: CloudSQLDatabase
    # TODO: this should really be
    primitive_type: PrimitiveType = PrimitiveType.GCP

    def sink_provider(self) -> PostgresTableProvider:
        return PostgresTableProvider(table=self.table, database=self.database)

    def _pulumi_provider(self) -> PostgresTableProvider:
        return PostgresTableProvider(table=self.table, database=self.database)
