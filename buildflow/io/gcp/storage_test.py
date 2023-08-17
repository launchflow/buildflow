import unittest
from dataclasses import dataclass

import pulumi
import pulumi_gcp
import pytest

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
@pytest.mark.usefixtures("event_loop_instance")
class GCSTest(unittest.TestCase):
    def get_async_result(self, coro):
        """Run a coroutine synchronously."""
        return self.event_loop.run_until_complete(coro)

    @pulumi.runtime.test
    def test_gcs_bucket_pulumi_base(self):
        gcs_bucket = GCSBucket(
            project_id="project_id",
            bucket_name="bucket_name",
        ).options(managed=True, bucket_region="US")

        bucket_resource = gcs_bucket.pulumi_provider().pulumi_resource(
            type_=None,
            credentials=EmptyCredentials(None),
            opts=pulumi.ResourceOptions(),
        )

        self.assertIsNotNone(bucket_resource)

        child_resources = bucket_resource._childResources
        self.assertEqual(len(child_resources), 1)
        self.assertIsInstance(list(child_resources)[0], pulumi_gcp.storage.Bucket)


if __name__ == "__main__":
    unittest.main()
