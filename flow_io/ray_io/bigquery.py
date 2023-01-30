"""IO connectors for Bigquery and Ray."""

from typing import Any, Dict

from google.cloud import bigquery
import ray


def _get_bigquery_client():
    return bigquery.Client()


@ray.remote
class RedisStreamOutput:

    def __init__(
        self,
        config: Dict[str, Any],
        bigquery_client=None,
    ) -> None:
        if bigquery_client is None:
            bigquery_client = _get_bigquery_client()
        self.bigquery_client = bigquery_client
        self.bigquery_table = config['bigquery_table']

    def write(self, element: Dict):
        self.bigquery_client.insert_rows(self.bigquery_table, [element])
