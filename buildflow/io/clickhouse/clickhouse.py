import dataclasses
from typing import Optional, Type

from buildflow.config.cloud_provider_config import LocalOptions
from buildflow.core.credentials import EmptyCredentials
from buildflow.core.types.clickhouse_types import (
    ClickhouseDatabase,
    ClickhouseHost,
    ClickhousePassword,
    ClickhouseTableID,
    ClickhouseUsername,
)
from buildflow.io.clickhouse.strategies.clickhouse_strategies import ClickhouseSink
from buildflow.io.primitive import LocalPrimtive, Primitive

_DEFAULT_DESTROY_PROTECTION = False
_DEFAULT_BATCH_SIZE = 10_000


@dataclasses.dataclass
class ClickhouseTable(LocalPrimtive):
    database: ClickhouseDatabase
    table: ClickhouseTableID
    schema: Optional[Type] = dataclasses.field(default=None, init=False)
    # Arguments for authentication
    host: ClickhouseHost
    username: ClickhouseUsername
    password: ClickhousePassword

    def options(
        self,
        # Pulumi management options
        destroy_protection: bool = _DEFAULT_DESTROY_PROTECTION,
        schema: Optional[Type] = None,
        # Sink options
        batch_size: int = _DEFAULT_BATCH_SIZE,
    ) -> Primitive:
        self.destroy_protection = destroy_protection
        self.batch_size = batch_size
        self.schema = schema
        return self

    @classmethod
    def from_local_options(
        cls,
        local_options: LocalOptions,
        *,
        host: ClickhouseHost,
        username: ClickhouseUsername,
        password: ClickhousePassword,
        database: ClickhouseDatabase,
        table: ClickhouseTableID,
    ) -> "LocalPrimtive":
        """Create a primitive from LocalOptions."""
        return cls(host, username, password, database, table)

    def sink(self, credentials: EmptyCredentials):
        return ClickhouseSink(
            credentials=credentials,
            host=self.host,
            username=self.username,
            password=self.password,
            database=self.database,
            table=self.table,
        )
