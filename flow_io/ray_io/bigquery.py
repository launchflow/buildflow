"""IO connectors for Bigquery and Ray."""

from typing import Any, Callable, Dict, Iterable, Union

import ray
from google.cloud import bigquery

from flow_io import resources
from flow_io.ray_io import base


def _get_bigquery_client():
    return bigquery.Client()


@ray.remote
class BigQuerySourceActor(base.RaySource):

    def __init__(
        self,
        ray_sinks: Iterable[base.RaySink],
        bq_ref: resources.BigQuery,
    ) -> None:
        super().__init__(ray_sinks)
        self.bq_client = _get_bigquery_client()
        self.query = bq_ref.query
        if not self.query:
            self.query = (
                'SELECT * FROM '
                f'`{bq_ref.project}.{bq_ref.dataset}.{bq_ref.table}`')

    def run(self):
        # TODO: it would be nice if we could shard up the reading
        # of the rows with ray. What if someone instantiates the
        # actor multiple times?
        query_job = self.bq_client.query(self.query)
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
        bq_ref: resources.BigQuery,
    ) -> None:
        super().__init__(remote_fn)
        self.bq_client = _get_bigquery_client()
        self.bq_table_id = f'{bq_ref.project}.{bq_ref.dataset}.{bq_ref.table}'

    def _write(
        self,
        element: Union[Dict[str, Any], Iterable[Dict[str, Any]]],
    ):
        to_insert = element
        if isinstance(element, dict):
            to_insert = [element]
        return self.bq_client.insert_rows(self.bq_table_id, to_insert)
