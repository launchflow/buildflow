import unittest
from dataclasses import dataclass
from unittest import mock

import pulumi
import pytest

from buildflow.core.credentials.empty_credentials import EmptyCredentials
from buildflow.io.gcp.bigquery_dataset import BigQueryDataset
from buildflow.io.gcp.bigquery_table import BigQueryTable


@dataclass
class FakeRow:
    value: int


class MyMocks(pulumi.runtime.Mocks):
    def new_resource(self, args: pulumi.runtime.MockResourceArgs):
        return [args.name + "_id", args.inputs]

    def call(self, args: pulumi.runtime.MockCallArgs):
        return {}


pulumi.runtime.set_mocks(
    MyMocks(),
    preview=False,
)


@pytest.mark.usefixtures("event_loop_instance")
class BigQueryTest(unittest.TestCase):
    def get_async_result(self, coro):
        """Run a coroutine synchronously."""
        return self.event_loop.run_until_complete(coro)

    @pulumi.runtime.test
    def test_bigquery_table_pulumi_base(self):
        bigquery_dataset = BigQueryDataset(
            project_id="project_id", dataset_name="dataset_name"
        )
        bigquery_table = BigQueryTable(
            bigquery_dataset, table_name="table_name"
        ).options(destroy_protection=False)

        bigquery_dataset_resource = bigquery_dataset._pulumi_provider().pulumi_resource(
            type_=FakeRow,
            credentials=EmptyCredentials(None),
            opts=pulumi.ResourceOptions(),
        )

        child_resources = list(bigquery_dataset_resource._childResources)
        self.assertEqual(len(child_resources), 1)

        dataset_resource = child_resources[0]

        def check_dataset(args):
            _, project, dataset_id = args
            self.assertEqual(project, "project_id")
            self.assertEqual(dataset_id, "dataset_name")

        pulumi.Output.all(
            dataset_resource.urn, dataset_resource.project, dataset_resource.dataset_id
        ).apply(check_dataset)

        bigquery_table_resource = bigquery_table._pulumi_provider().pulumi_resource(
            type_=FakeRow,
            credentials=EmptyCredentials(None),
            opts=pulumi.ResourceOptions(),
        )
        child_resources = list(bigquery_table_resource._childResources)
        self.assertEqual(len(child_resources), 1)

        table_resource = child_resources[0]

        def check_table(args):
            _, project, dataset_id, schema, delete_protect = args
            self.assertEqual(project, "project_id")
            self.assertEqual(dataset_id, "dataset_name")
            self.assertEqual(
                schema, '[{"name": "value", "type": "INTEGER", "mode": "REQUIRED"}]'
            )
            # Deletion protection is enabled by default
            self.assertEqual(delete_protect, False)

        pulumi.Output.all(
            table_resource.urn,
            table_resource.project,
            table_resource.dataset_id,
            table_resource.schema,
            table_resource.deletion_protection,
        ).apply(check_table)

    def test_bigquery_table_pulumi_no_protect(self):
        bigquery_table = BigQueryTable(
            BigQueryDataset(project_id="project_id", dataset_name="dataset_name"),
            table_name="table_name",
        )

        bigquery_resource = bigquery_table._pulumi_provider().pulumi_resource(
            type_=FakeRow,
            credentials=EmptyCredentials(None),
            opts=pulumi.ResourceOptions(),
        )
        child_resources = list(bigquery_resource._childResources)
        self.assertEqual(len(child_resources), 1)

        table_resource = child_resources[0]

        def check_table(args):
            _, delete_protect = args
            self.assertEqual(delete_protect, False)

        pulumi.Output.all(
            table_resource.urn,
            table_resource.deletion_protection,
        ).apply(check_table)

    @mock.patch("buildflow.io.utils.clients.gcp_clients.GCPClients")
    def test_bigquery_sink(self, gcp_client_mock: mock.MagicMock):
        insert_rows_mock = (
            gcp_client_mock.return_value.get_bigquery_client.return_value.insert_rows_json
        )
        insert_rows_mock.return_value = []

        bigquery_table = BigQueryTable(
            BigQueryDataset(project_id="project_id", dataset_name="dataset_name"),
            table_name="table_name",
        )
        bigquery_sink = bigquery_table.sink_provider().sink(mock.MagicMock())

        rows = [FakeRow(1)] * 20000
        self.get_async_result(bigquery_sink.push(rows))

        self.assertEqual(insert_rows_mock.call_count, 2)


if __name__ == "__main__":
    unittest.main()
