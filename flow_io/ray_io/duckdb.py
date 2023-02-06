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
        while True:
            element = self.duck_con.fetchone()
            if not element:
                break
            for ray_input in self.ray_inputs:
                refs.append(ray_input.remote(element))
        self.duck_con.close()
        return ray.get(refs)


@ray.remote
class DuckDBSinkActor:

    def __init__(
        self,
        database: str,
        table: str,
    ) -> None:
        self.database = database
        self.table = table

    def write(
        self,
        element: Union[Dict[str, Any], Iterable[Dict[str, Any]]],
    ):
        duck_con = duckdb.connect(database=self.database, read_only=False)
        if isinstance(element, dict):
            df = pd.DataFrame([element])
        else:
            df = pd.DataFrame(element)
        while True:
            try:
                duck_con.append(self.table, df)
                break
            except duckdb.CatalogException:
                # This can happen if the table doesn't exist yet. If this
                # happen create it from the DF.
                duck_con.execute(
                    f'CREATE TABLE {self.table} AS SELECT * FROM df')
                break
            except duckdb.IOException:
                logging.warning(
                    'Can\'t concurrently write to DuckDB waiting 2 '
                    'seconds then will try again.'
                )
                time.sleep(2)
        duck_con.close()
        return
