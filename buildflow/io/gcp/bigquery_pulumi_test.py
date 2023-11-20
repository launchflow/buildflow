import unittest
from dataclasses import dataclass

import pulumi

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


class BigQueryPulumiTest(unittest.TestCase):
    @pulumi.runtime.test
    def test_bigquery_table_pulumi_base(self):
        bigquery_dataset = BigQueryDataset(
            project_id="project_id", dataset_name="dataset_name"
        )
        bigquery_table = BigQueryTable(
            bigquery_dataset, table_name="table_name"
        ).options(destroy_protection=False, schema=FakeRow)

        bigquery_dataset_resources = bigquery_dataset.pulumi_resources(
            credentials=EmptyCredentials(),
            opts=pulumi.ResourceOptions(),
        )

        self.assertEqual(len(bigquery_dataset_resources), 1)

        dataset_resource = bigquery_dataset_resources[0]

        def check_dataset(args):
            _, project, dataset_id = args
            self.assertEqual(project, "project_id")
            self.assertEqual(dataset_id, "dataset_name")

        pulumi.Output.all(
            dataset_resource.urn, dataset_resource.project, dataset_resource.dataset_id
        ).apply(check_dataset)

        bigquery_table_resource = bigquery_table.pulumi_resources(
            credentials=EmptyCredentials(),
            opts=pulumi.ResourceOptions(),
        )
        self.assertEqual(len(bigquery_table_resource), 1)

        table_resource = bigquery_table_resource[0]

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

    @pulumi.runtime.test
    def test_bigquery_table_pulumi_no_protect(self):
        bigquery_table = BigQueryTable(
            BigQueryDataset(project_id="project_id", dataset_name="dataset_name"),
            table_name="table_name",
        ).options(schema=FakeRow, destroy_protection=False)

        bigquery_resources = bigquery_table.pulumi_resources(
            credentials=EmptyCredentials(),
            opts=pulumi.ResourceOptions(),
        )
        self.assertEqual(len(bigquery_resources), 1)

        table_resource = bigquery_resources[0]

        def check_table(args):
            _, delete_protect = args
            self.assertEqual(delete_protect, False)

        pulumi.Output.all(
            table_resource.urn,
            table_resource.deletion_protection,
        ).apply(check_table)


if __name__ == "__main__":
    unittest.main()
