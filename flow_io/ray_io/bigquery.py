"""IO connectors for Bigquery and Ray."""

from typing import Dict, Iterable

from google.cloud import bigquery
import ray


def _get_bigquery_client():
    return bigquery.Client()


class BigQueryInput:

    def __init__(
        self,
        ray_dags: Iterable,
        project: str,
        dataset: str,
        table: str,
        query: str,
        bigquery_client=None,
    ) -> None:
        if bigquery_client is None:
            bigquery_client = _get_bigquery_client()
        self.bigquery_client = bigquery_client
        self.query = query
        self.ray_dags = ray_dags

    def run(self):
        query_job = self.bigquery_client.query(self.query)
        for row in query_job.result():
            for ray_dag in self.ray_dags:
                ray_dag.execute(row)


@ray.remote
class BigQueryOutput:

    def __init__(
        self,
        project: str,
        dataset: str,
        table: str,
        query: str,
        bigquery_client=None,
    ) -> None:
        if bigquery_client is None:
            bigquery_client = _get_bigquery_client()
        self.bigquery_client = bigquery_client
        self.bigquery_table = f'{project}.{dataset}.{table}'

    def write(self, element: Dict):
        self.bigquery_client.insert_rows(self.bigquery_table, [element])
