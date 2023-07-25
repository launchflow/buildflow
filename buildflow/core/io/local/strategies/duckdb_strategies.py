import asyncio
import duckdb
import logging
import pandas as pd
from typing import Any, Callable, Dict, Iterable, Type

from buildflow.core.credentials import EmptyCredentials
from buildflow.core.io.utils.schemas import converters
from buildflow.core.strategies.sink import SinkStrategy
from buildflow.core.types.local_types import DuckDBDatabase, DuckDBTable


_MAX_CONNECT_TRIES = 25


class DuckDBSink(SinkStrategy):
    def __init__(
        self,
        *,
        credentials: EmptyCredentials,
        database: DuckDBDatabase,
        table: DuckDBTable,
    ):
        super().__init__(credentials=credentials, strategy_id="local-duckdb-sink")
        self.database = database
        self.table = table

    def push_converter(
        self, user_defined_type: Type
    ) -> Callable[[Any], Dict[str, Any]]:
        return converters.json_push_converter(user_defined_type)

    async def push(self, batch: Iterable[Dict[str, Any]]):
        df = pd.DataFrame(batch)
        connect_tries = 0
        while connect_tries < _MAX_CONNECT_TRIES:
            try:
                with duckdb.connect(self.database, read_only=False) as con:
                    try:
                        con.append(self.table, df)
                    except duckdb.CatalogException:
                        con.execute(f"CREATE TABLE {self.table} AS SELECT * FROM df")
                    break
            except duckdb.IOException:
                logging.exception("failed to connect to duckdb database")
                connect_tries += 1
                if connect_tries == _MAX_CONNECT_TRIES:
                    raise ValueError(
                        "failed to connect to duckdb database. did you leave a "
                        "connection open?"
                    )
                else:
                    logging.warning(
                        "can't concurrently write to DuckDB waiting 2 seconds then will"
                        " try again"
                    )
                    await asyncio.sleep(2)