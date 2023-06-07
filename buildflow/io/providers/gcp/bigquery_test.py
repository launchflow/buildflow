"""Tests for bigquery.py

TODO: these tests don't actually validate the pulumi resources unfortunately.

The `@pulumi.runtime.test` annotation only awaits what is returned so need
to refactor this. It's still worth keeping the tests though cause they
do test the basics of pulumi resources.

Actually maybe they are? it's very unclear.. but need to dig into it more
"""

from dataclasses import dataclass
import unittest
from unittest import mock

import pulumi
import pytest

from buildflow.io.providers.gcp.bigquery import StreamingBigQueryProvider


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
        provider = StreamingBigQueryProvider(
            project_id="test", table_id="project.ds.table"
        )

        pulumi_resources = provider.pulumi(type_=FakeRow)

        resources = pulumi_resources.resources
        exports = pulumi_resources.exports

        self.assertEqual(len(resources), 2)

        dataset_resource = resources[0]
        table_resource = resources[1]

        def check_dataset(args):
            _, project, dataset_id = args
            self.assertEqual(project, "project")
            self.assertEqual(dataset_id, "ds")

        pulumi.Output.all(
            dataset_resource.urn, dataset_resource.project, dataset_resource.dataset_id
        ).apply(check_dataset)
        self.assertEqual(dataset_resource._childResources, {table_resource})

        def check_table(args):
            _, project, dataset_id, schema, delete_protect = args
            self.assertEqual(project, "project")
            self.assertEqual(dataset_id, "ds")
            self.assertEqual(
                schema, '[{"name": "value", "type": "INTEGER", "mode": "REQUIRED"}]'
            )
            self.assertEqual(delete_protect, None)

        pulumi.Output.all(
            table_resource.urn,
            table_resource.project,
            table_resource.dataset_id,
            table_resource.schema,
            table_resource.deletion_protection,
        ).apply(check_table)

        self.assertEqual(
            exports,
            {
                "gcp.bigquery.dataset_id": "project.ds",
                "gcp.biquery.table_id": "project.ds.table",
            },
        )

    def test_bigquery_table_pulumi_no_protect(self):
        provider = StreamingBigQueryProvider(
            project_id="test",
            table_id="project.ds.table",
            destroy_protection=False,
        )

        pulumi_resources = provider.pulumi(type_=FakeRow)

        resources = pulumi_resources.resources

        self.assertEqual(len(resources), 2)

        table_resource = resources[1]

        def check_table(args):
            _, delete_protect = args
            self.assertEqual(delete_protect, False)

        pulumi.Output.all(
            table_resource.urn,
            table_resource.deletion_protection,
        ).apply(check_table)

    def test_bigquery_table_pulumi_no_dataset(self):
        provider = StreamingBigQueryProvider(
            project_id="test",
            table_id="project.ds.table",
            include_dataset=False,
        )

        pulumi_resources = provider.pulumi(type_=FakeRow)

        resources = pulumi_resources.resources
        exports = pulumi_resources.exports

        self.assertEqual(len(resources), 1)

        table_resource = resources[0]

        def check_table(args):
            _, project, dataset_id, schema, delete_protect = args
            self.assertEqual(project, "project")
            self.assertEqual(dataset_id, "ds")
            self.assertEqual(
                schema, '[{"name": "value", "type": "INTEGER", "mode": "REQUIRED"}]'
            )
            self.assertEqual(delete_protect, None)

        pulumi.Output.all(
            table_resource.urn,
            table_resource.project,
            table_resource.dataset_id,
            table_resource.schema,
            table_resource.deletion_protection,
        ).apply(check_table)

        self.assertFalse(hasattr(table_resource, "opts"))
        self.assertNotIn("gcp.bigquery.dataset_id", exports)

    @mock.patch("buildflow.io.providers.gcp.utils.clients.get_bigquery_client")
    def test_bigquery_push(self, bq_client_mock: mock.MagicMock):
        insert_rows_mock = bq_client_mock.return_value.insert_rows_json
        insert_rows_mock.return_value = []

        provider = StreamingBigQueryProvider(
            project_id="test", table_id="project.ds.table"
        )

        rows = [FakeRow(1)] * 20000
        self.get_async_result(provider.push(rows))

        self.assertEqual(insert_rows_mock.call_count, 2)


if __name__ == "__main__":
    unittest.main()
