import asyncio
import logging
from typing import Any, Callable, Dict, Iterable, Type

import clickhouse_connect
import pandas as pd

from buildflow.core.credentials import EmptyCredentials
from buildflow.core.types.clickhouse_types import (
    ClickhouseDatabase,
    ClickhouseHost,
    ClickhousePassword,
    ClickhouseTableID,
    ClickhouseUsername,
)
from buildflow.io.strategies.sink import SinkStrategy
from buildflow.io.utils.schemas import converters

_MAX_CONNECT_TRIES = 25


class ClickhouseSink(SinkStrategy):
    def __init__(
        self,
        *,
        credentials: EmptyCredentials,
        host: ClickhouseHost,
        username: ClickhouseUsername,
        password: ClickhousePassword,
        database: ClickhouseDatabase,
        table: ClickhouseTableID,
    ):
        super().__init__(credentials=credentials, strategy_id="clickhouse-sink")
        self.client = None
        self.database = database
        self.table = table
        self.host = host
        self.username = username
        self.password = password

    async def connect(self):
        connect_tries = 0
        while connect_tries < _MAX_CONNECT_TRIES:
            try:
                self.client = clickhouse_connect.get_client(
                    host=self.host,
                    username=self.username,
                    password=self.password,
                )
                self.client.command(
                    f"CREATE DATABASE IF NOT EXISTS {self.database} ENGINE = Memory"
                )
                self.client.command(f'USE "{self.database}"')
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
        if self.client is None:
            await self.connect()
        df = pd.DataFrame(batch)
        try:
            self.client.insert_df(self.table, df)
        except clickhouse_connect.driver.exceptions.DataError:
            logging.exception("failed to connect to clickhouse database")
