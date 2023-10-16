import asyncio
import logging
from typing import Any, Callable, Dict, Iterable, Type

import clickhouse_connect
import pandas as pd

from buildflow.core.credentials import ClickhouseCredentials
from buildflow.core.types.clickhouse_types import ClickhouseDatabase, ClickhouseTableID
from buildflow.io.strategies.sink import SinkStrategy
from buildflow.io.utils.schemas import converters

_MAX_CONNECT_TRIES = 25


class ClickhouseSink(SinkStrategy):
    def __init__(
        self,
        *,
        credentials: ClickhouseCredentials,
        database: ClickhouseDatabase,
        table: ClickhouseTableID,
    ):
        super().__init__(credentials=credentials, strategy_id="local-clickhouse-sink")
        self.client = None
        self.database = database
        self.table = table
        self.connect(credentials)

    async def connect(self, credentials: ClickhouseCredentials):
        connect_tries = 0
        while connect_tries < _MAX_CONNECT_TRIES:
            try:
                self.client = clickhouse_connect.get_client(
                    host=credentials.host,
                    username=credentials.username,
                    password=credentials.password,
                )
                break
            except clickhouse_connect.driver.exceptions.Error:
                logging.exception("failed to connect to clickhouse database")
                connect_tries += 1
                if connect_tries == _MAX_CONNECT_TRIES:
                    raise ValueError(
                        "failed to connect to clickhouse database. did you leave a "
                        "connection open?"
                    )
                else:
                    logging.warning(
                        "can't concurrently write to Clickhouse "
                        "waiting 2 seconds then will "
                        "try again"
                    )
                    await asyncio.sleep(2)

    def push_converter(
        self, user_defined_type: Type
    ) -> Callable[[Any], Dict[str, Any]]:
        return converters.json_push_converter(user_defined_type)

    async def push(self, batch: Iterable[Dict[str, Any]]):
        df = pd.DataFrame(batch)
        try:
            self.client.insert_df(self.table, df)
        except clickhouse_connect.driver.exceptions.DataError:
            logging.exception("failed to connect to duckdb database")
