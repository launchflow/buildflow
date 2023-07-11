import unittest
from dataclasses import dataclass
from typing import List
from unittest import mock

import pulumi
import pytest

from buildflow.core.io.gcp.bigquery import BigQueryTable
from buildflow.core.resources.pulumi import PulumiResource


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
        bigquery_table = BigQueryTable(
            project_id="project_id",
            dataset_name="dataset_name",
            table_name="table_name",
        )

        pulumi_resources: List[
            PulumiResource
        ] = bigquery_table.pulumi_provider().pulumi_resources(type_=FakeRow)
        self.assertEqual(len(pulumi_resources), 2)

        all_exports = {}
        for resource in pulumi_resources:
            all_exports.update(resource.exports)

        dataset_resource = pulumi_resources[0].resource
        table_resource = pulumi_resources[1].resource

        def check_dataset(args):
            _, project, dataset_id = args
            self.assertEqual(project, "project_id")
            self.assertEqual(dataset_id, "dataset_name")

        pulumi.Output.all(
            dataset_resource.urn, dataset_resource.project, dataset_resource.dataset_id
        ).apply(check_dataset)
        self.assertEqual(dataset_resource._childResources, {table_resource})

        def check_table(args):
            _, project, dataset_id, schema, delete_protect = args
            self.assertEqual(project, "project_id")
            self.assertEqual(dataset_id, "dataset_name")
            self.assertEqual(
                schema, '[{"name": "value", "type": "INTEGER", "mode": "REQUIRED"}]'
            )
            self.assertEqual(delete_protect, False)

        pulumi.Output.all(
            table_resource.urn,
            table_resource.project,
            table_resource.dataset_id,
            table_resource.schema,
            table_resource.deletion_protection,
        ).apply(check_table)

        self.assertEqual(
            all_exports,
            {
                "gcp.bigquery.dataset_id": "project_id.dataset_name",
                "gcp.bigquery.table_id": "project_id.dataset_name.table_name",
            },
        )

    def test_bigquery_table_pulumi_no_protect(self):
        # DO NOT SUBMIT: Support setting deletion field and not setting dataset and
        # update this test
        bigquery_table = BigQueryTable(
            project_id="project_id",
            dataset_name="dataset_name",
            table_name="table_name",
        )

        pulumi_resources: List[
            PulumiResource
        ] = bigquery_table.pulumi_provider().pulumi_resources(type_=FakeRow)
        self.assertEqual(len(pulumi_resources), 2)

        table_resource = pulumi_resources[1].resource

        def check_table(args):
            _, delete_protect = args
            self.assertEqual(delete_protect, False)

        pulumi.Output.all(
            table_resource.urn,
            table_resource.deletion_protection,
        ).apply(check_table)

    @mock.patch("buildflow.core.io.utils.clients.gcp_clients.get_bigquery_client")
    def test_bigquery_sink(self, bq_client_mock: mock.MagicMock):
        insert_rows_mock = bq_client_mock.return_value.insert_rows_json
        insert_rows_mock.return_value = []

        bigquery_table = BigQueryTable(
            project_id="project_id",
            dataset_name="dataset_name",
            table_name="table_name",
        )
        bigquery_sink = bigquery_table.sink_provider().sink()

        rows = [FakeRow(1)] * 20000
        self.get_async_result(bigquery_sink.push(rows))

        self.assertEqual(insert_rows_mock.call_count, 2)


if __name__ == "__main__":
    unittest.main()
