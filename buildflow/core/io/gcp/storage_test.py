import unittest
from dataclasses import dataclass
from typing import List

import pulumi
import pytest

from buildflow.core.io.gcp.storage import GCSBucket
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
            bucket_region="bucket_region",
        )

        pulumi_resources: List[
            PulumiResource
        ] = gcs_bucket.pulumi_provider().pulumi_resources(type_=None)
        self.assertEqual(len(pulumi_resources), 1)

        all_exports = {}
        for resource in pulumi_resources:
            all_exports.update(resource.exports)

        gcs_bucket_resource = pulumi_resources[0].resource

        def check_bucket(args):
            _, project, name, location = args
            self.assertEqual(project, "project_id")
            self.assertEqual(name, "bucket_name")
            self.assertEqual(location, "bucket_region")

        pulumi.Output.all(
            gcs_bucket_resource.urn,
            gcs_bucket_resource.project,
            gcs_bucket_resource.name,
            gcs_bucket_resource.location,
        ).apply(check_bucket)

        self.assertEqual(
            all_exports,
            {
                "gcp.storage.bucket_id": "bucket_name",
            },
        )


if __name__ == "__main__":
    unittest.main()
