import dataclasses
from typing import Optional, Type

from buildflow.core.types.clickhouse_types import ClickhouseDatabase, ClickhouseTableID
from buildflow.io.clickhouse.providers.clickhouse_providers import ClickhouseProvider
from buildflow.io.primitive import LocalPrimtive, Primitive

_DEFAULT_DESTROY_PROTECTION = False
_DEFAULT_BATCH_SIZE = 10_000


class ClickhouseTable(
    LocalPrimtive[
        # Pulumi provider type
        None,
        # Source provider type
        None,
        # Sink provider type
        ClickhouseProvider,
        # Background task provider type
        None,
    ]
):
    database: ClickhouseDatabase
    table: ClickhouseTableID
    schema: Optional[Type] = dataclasses.field(default=None, init=False)

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

    def sink_provider(self) -> ClickhouseProvider:
        return ClickhouseProvider(database=self.database, table=self.table)

    def _pulumi_provider(self) -> ClickhouseProvider:
        return ClickhouseProvider(database=self.database, table=self.table)
