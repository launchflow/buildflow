"""IO connectors for Bigquery and Ray."""

from typing import Dict, Iterable

from google.cloud import bigquery
import ray

from flow_io.ray_io import base


def _get_bigquery_client():
    return bigquery.Client()


@ray.remote
class BigQuerySourceActor(base.RaySource):

    def __init__(
        self,
        ray_inputs: Iterable,
        input_node_space: str,
        project: str,
        dataset: str,
        table: str,
        query: str,
        bigquery_client=None,
    ) -> None:
        super().__init__(ray_inputs, input_node_space)
        if bigquery_client is None:
            bigquery_client = _get_bigquery_client()
        self.bigquery_client = bigquery_client
        self.query = query

    def run(self):
        query_job = self.bigquery_client.query(self.query)
        refs = []
        for row in query_job.result():
            for ray_input in self.ray_inputs:
                refs.append(ray_input.remote(row))
        return ray.get(refs)


@ray.remote
class BigQuerySinkActor:

    def __init__(
        self,
        project: str,
        dataset: str,
        table: str,
        bigquery_client=None,
    ) -> None:
        if bigquery_client is None:
            bigquery_client = _get_bigquery_client()
        self.bigquery_client = bigquery_client
        self.bigquery_table = f'{project}.{dataset}.{table}'

    def write(self, element: Dict):
        return self.bigquery_client.insert_rows(self.bigquery_table, [element])
