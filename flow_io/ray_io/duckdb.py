"""IO connectors for DuckDB and Ray."""

import logging
from multiprocessing import connection
import time
from typing import Any, Callable, Dict, Iterable, Union

import duckdb
import pandas as pd
import ray

from flow_io import resources
from flow_io.ray_io import base


@ray.remote
class DuckDBQueryActor:

    def __init__(self, database: str, query: str, table: str) -> None:
        if not query:
            query = f'SELECT * FROM {table}'
        self.query = query
        self.database = database
        self.connection = None

    def connect(self):
        self.connection = duckdb.connect(self.database, read_only=True)
        self.connection.execute(self.query)

    def get_connection(self):
        if self.connection is None:
            raise ValueError('connection not setup.')
        return self.connection


@ray.remote
class DuckDBSourceActor(base.RaySource):

    def __init__(
        self,
        ray_sinks: Iterable[base.RaySink],
        query_actor: DuckDBQueryActor,
    ) -> None:
        super().__init__(ray_sinks)
        self.query_actor = query_actor

    @classmethod
    def actor_inputs(cls, io_ref):
        query_actor = DuckDBQueryActor.remote(io_ref.database, io_ref.query,
                                              io_ref.table)
        ray.get(query_actor.connect.remote())
        return [query_actor]

    def run(self):
        refs = []
        conn = ray.get(self.query_actor.get_connection.remote())
        df = conn.fetch_df_chunk()
        while not df.empty:
            elements = df.to_dict('records')
            for ray_sink in self.ray_sinks:
                for element in elements:
                    refs.append(ray_sink.write.remote(element))
            df = self.duck_con.fetch_df_chunk()
        self.duck_con.close()
        return ray.get(refs)


_MAX_CONNECT_TRIES = 20


@ray.remote
class DuckDBSinkActor(base.RaySink):

    def __init__(
        self,
        remote_fn: Callable,
        duckdb_ref: resources.DuckDB,
    ) -> None:
        super().__init__(remote_fn)
        self.database = duckdb_ref.database
        self.table = duckdb_ref.table

    def _write(
        self,
        element: Union[Dict[str, Any], Iterable[Dict[str, Any]]],
    ):

        connect_tries = 0
        while connect_tries < _MAX_CONNECT_TRIES:
            try:
                duck_con = duckdb.connect(database=self.database,
                                          read_only=False)
                break
            except duckdb.IOException as e:
                if 'Could not set lock on file' in str(e):
                    connect_tries += 1
                    if connect_tries == _MAX_CONNECT_TRIES:
                        raise ValueError(
                            'Failed to connect to duckdb. Did you leave a '
                            'connection open?') from e
                    logging.warning(
                        'Can\'t concurrently write to DuckDB waiting 2 '
                        'seconds then will try again.')
                    time.sleep(2)
                else:
                    raise e
        if isinstance(element, dict):
            df = pd.DataFrame([element])
        else:
            df = pd.DataFrame(element)
        try:
            duck_con.append(self.table, df)
        except duckdb.CatalogException:
            # This can happen if the table doesn't exist yet. If this
            # happen create it from the DF.
            duck_con.execute(f'CREATE TABLE {self.table} AS SELECT * FROM df')
        duck_con.close()
        return
