import unittest
from dataclasses import dataclass
from unittest import mock

from buildflow.io.gcp.bigquery_dataset import BigQueryDataset
from buildflow.io.gcp.bigquery_table import BigQueryTable


@dataclass
class FakeRow:
    value: int


class BigQueryTest(unittest.IsolatedAsyncioTestCase):
    @mock.patch("buildflow.io.utils.clients.gcp_clients.GCPClients")
    async def test_bigquery_sink(self, gcp_client_mock: mock.MagicMock):
        insert_rows_mock = (
            gcp_client_mock.return_value.get_bigquery_client.return_value.insert_rows_json
        )
        insert_rows_mock.return_value = []

        bigquery_table = BigQueryTable(
            BigQueryDataset(project_id="project_id", dataset_name="dataset_name"),
            table_name="table_name",
        )
        bigquery_sink = bigquery_table.sink(mock.MagicMock())

        rows = [FakeRow(1)] * 20000
        await bigquery_sink.push(rows)

        self.assertEqual(insert_rows_mock.call_count, 2)


if __name__ == "__main__":
    unittest.main()
