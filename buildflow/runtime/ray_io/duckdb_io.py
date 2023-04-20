"""IO connectors for DuckDB and Ray."""

import dataclasses
import logging
import time
from typing import Any, Callable, Dict, Iterable, Union

import duckdb
import pandas as pd
import ray

from buildflow.api import io
from buildflow.runtime.ray_io import base


@dataclasses.dataclass
class DuckDBSource(io.Source):
    """Source for reading from DuckDB."""
    database: str = ''
    query: str = ''
    table: str = ''

    def actor(self, ray_sinks):
        return DuckDBSourceActor.remote(ray_sinks, self)


@dataclasses.dataclass
class DuckDBSink(io.Sink):
    """Sink for writing to a table in a DuckDB database."""
    database: str
    table: str

    def actor(self, remote_fn: Callable, is_streaming: bool):
        return DuckDBSinkActor.remote(remote_fn, self)


@ray.remote(num_cpus=DuckDBSource.num_cpus())
class DuckDBSourceActor(base.RaySource):

    def __init__(
        self,
        ray_sinks: Iterable[base.RaySink],
        duckdb_ref: DuckDBSource,
    ) -> None:
        super().__init__(ray_sinks)
        self.duck_con = duckdb.connect(database=duckdb_ref.database,
                                       read_only=True)
        query = duckdb_ref.query
        if not query:
            query = f'SELECT * FROM {duckdb_ref.table}'
        self.duck_con.execute(query=query)

    def run(self):
        refs = []
        df = self.duck_con.fetch_df_chunk()
        while not df.empty:
            elements = df.to_dict('records')
            for ray_sink in self.ray_sinks:
                for element in elements:
                    refs.append(ray_sink.write.remote(element))
            df = self.duck_con.fetch_df_chunk()
        self.duck_con.close()
        return ray.get(refs)


_MAX_CONNECT_TRIES = 20


@ray.remote(num_cpus=DuckDBSource.num_cpus())
class DuckDBSinkActor(base.RaySink):

    def __init__(
        self,
        remote_fn: Callable,
        duckdb_ref: DuckDBSink,
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
