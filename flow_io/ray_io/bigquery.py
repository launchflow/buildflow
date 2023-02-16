"""IO connectors for Bigquery and Ray."""

from typing import Any, Callable, Dict, Iterable, Union

import ray
from google.cloud import bigquery

from flow_io.ray_io import base


def _get_bigquery_client():
    return bigquery.Client()


@ray.remote
class BigQuerySourceActor(base.RaySource):

    def __init__(
        self,
        ray_sinks: Iterable[base.RaySink],
        project: str,
        dataset: str,
        table: str,
        query: str = '',
        bigquery_client=None,
    ) -> None:
        super().__init__(ray_sinks)
        if bigquery_client is None:
            bigquery_client = _get_bigquery_client()
        self.bigquery_client = bigquery_client
        if not query:
            query = f'SELECT * FROM `{project}.{dataset}.{table}`'
        self.query = query

    def run(self):
        # TODO: it would be nice if we could shard up the reading
        # of the rows with ray. What if someone instantiates the
        # actor multiple times?
        query_job = self.bigquery_client.query(self.query)
        refs = []
        for row in query_job.result():
            for ray_sink in self.ray_sinks:
                refs.append(ray_sink.write.remote(row))
        return ray.get(refs)


@ray.remote
class BigQuerySinkActor(base.RaySink):

    def __init__(
        self,
        remote_fn: Callable,
        project: str,
        dataset: str,
        table: str,
        bigquery_client=None,
    ) -> None:
        super().__init__(remote_fn)
        if bigquery_client is None:
            bigquery_client = _get_bigquery_client()
        self.bigquery_client = bigquery_client
        self.bigquery_table = f'{project}.{dataset}.{table}'

    def _write(
        self,
        element: Union[Dict[str, Any], Iterable[Dict[str, Any]]],
    ):
        to_insert = element
        if isinstance(element, dict):
            to_insert = [element]
        return self.bigquery_client.insert_rows(self.bigquery_table, to_insert)
