"""IO connectors for DuckDB and Ray."""

import logging
import time
from typing import Any, Dict, Iterable, Union

import duckdb
import pandas as pd
import ray

from flow_io.ray_io import base


@ray.remote
class DuckDBSourceActor(base.RaySource):

    def __init__(
        self,
        ray_inputs: Iterable,
        input_node_space: str,
        database: str,
        table: str,
        query: str = '',
    ) -> None:
        super().__init__(ray_inputs, input_node_space)
        self.duck_con = duckdb.connect(database=database, read_only=True)
        if not query:
            query = f'SELECT * FROM {table}'
        self.duck_con.execute(query=query)

    def run(self):
        refs = []
        df = self.duck_con.fetch_df_chunk()
        while not df.empty:
            elements = df.to_dict('records')
            for ray_input in self.ray_inputs:
                for element in elements:
                    refs.append(ray_input.remote(element))
            df = self.duck_con.fetch_df_chunk()
        self.duck_con.close()
        return ray.get(refs)


_MAX_CONNECT_TRIES = 20


@ray.remote
class DuckDBSinkActor(base.RaySink):

    def __init__(
        self,
        database: str,
        table: str,
    ) -> None:
        super().__init__()
        self.database = database
        self.table = table

    def _write(
        self,
        element: Union[Dict[str, Any], Iterable[Dict[str, Any]]],
        carrier: Dict[str, str],
    ):

        def add_trace_info(elem: Dict[str, Any]):
            if 'trace_id' not in elem:
                elem['trace_id'] = carrier['trace_id']
            else:
                logging.warning(
                    'Cannot add trace_id to element. Key is already in use.')
            return elem

        connect_tries = 0
        while connect_tries < _MAX_CONNECT_TRIES:
            try:
                duck_con = duckdb.connect(database=self.database,
                                          read_only=False)
                break
            except duckdb.IOException as e:
                connect_tries += 1
                if connect_tries == _MAX_CONNECT_TRIES:
                    raise ValueError(
                        'Failed to connect to duckdb. Did you leave a '
                        'connection open?') from e
                logging.warning(
                    'Can\'t concurrently write to DuckDB waiting 2 '
                    'seconds then will try again.')
                time.sleep(2)
        if isinstance(element, dict):
            df = pd.DataFrame([add_trace_info(element)])
        else:
            df = pd.DataFrame([add_trace_info(elem) for elem in element])
        try:
            duck_con.append(self.table, df)
        except duckdb.CatalogException:
            # This can happen if the table doesn't exist yet. If this
            # happen create it from the DF.
            duck_con.execute(f'CREATE TABLE {self.table} AS SELECT * FROM df')
        duck_con.close()
        return
