import unittest
from dataclasses import dataclass

import pulumi
import pulumi_gcp

from buildflow.core.credentials.empty_credentials import EmptyCredentials
from buildflow.io.gcp.storage import GCSBucket


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


# TODO: Add tests for Sink methods. Can reference bigquery_test.py for an example.
class GCSTest(unittest.TestCase):
    @pulumi.runtime.test
    def test_gcs_bucket_pulumi_base(self):
        gcs_bucket = GCSBucket(
            project_id="project_id",
            bucket_name="bucket_name",
        ).options(bucket_region="US")
        gcs_bucket.enable_managed()

        bucket_resources = gcs_bucket.pulumi_resources(
            credentials=EmptyCredentials(),
            opts=pulumi.ResourceOptions(),
        )

        self.assertEqual(len(bucket_resources), 1)
        self.assertIsInstance(bucket_resources[0], pulumi_gcp.storage.Bucket)


if __name__ == "__main__":
    unittest.main()
