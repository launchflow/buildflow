"""IO connectors for Bigquery and Ray."""

from typing import Any, Dict, Iterable, Union

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
        query: str = '',
        bigquery_client=None,
    ) -> None:
        super().__init__(ray_inputs, input_node_space)
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
            for ray_input in self.ray_inputs:
                refs.append(ray_input.remote(row))
        return ray.get(refs)


@ray.remote
class BigQuerySinkActor(base.RaySink):

    def __init__(
        self,
        project: str,
        dataset: str,
        table: str,
        bigquery_client=None,
    ) -> None:
        super().__init__()
        if bigquery_client is None:
            bigquery_client = _get_bigquery_client()
        self.bigquery_client = bigquery_client
        self.bigquery_table = f'{project}.{dataset}.{table}'

    def _write(
        self,
        element: Union[Dict[str, Any], Iterable[Dict[str, Any]]],
        carrier: Dict[str, str],
    ):
        # TODO: add tracing
        del carrier
        to_insert = element
        if isinstance(element, dict):
            to_insert = [element]
        return self.bigquery_client.insert_rows(self.bigquery_table, to_insert)
