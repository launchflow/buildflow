"""Tests for bigquery.py"""

from dataclasses import dataclass
import unittest

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


@pytest.fixture(scope="class")
def pulumi_mocks(request):
    """Add the pulumi_mocks as an attribute to the unittest style test class."""
    pulumi.runtime.set_mocks(
        MyMocks(),
        preview=False,
    )


pulumi.runtime.set_mocks(
    MyMocks(),
    preview=False,
)


class BigQueryTest(unittest.TestCase):
    @pulumi.runtime.test
    def test_bigquery_table_pulumi_base(self):
        provider = StreamingBigQueryProvider(
            billing_project_id="test", table_id="project.ds.table"
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
                "gcp.bigquery.schema.project.ds.table": (
                    '[{"name": "value", "type": "INTEGER", "mode": "REQUIRED"}]'
                ),
            },
        )

    def test_bigquery_table_pulumi_no_protect(self):
        provider = StreamingBigQueryProvider(
            billing_project_id="test",
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
            table_resource.deletion_protection,
        ).apply(check_table)

    def test_bigquery_table_pulumi_no_dataset(self):
        provider = StreamingBigQueryProvider(
            billing_project_id="test",
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


if __name__ == "__main__":
    unittest.main()
