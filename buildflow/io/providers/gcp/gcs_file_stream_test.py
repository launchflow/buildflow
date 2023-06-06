"""Tests for gcs_file_stream.py

TODO: these tests don't actually validate the pulumi resources unfortunately.

The `@pulumi.runtime.test` annotation only awaits what is returned so need
to refactor this. It's still worth keeping the tests though cause they
do test the basics of pulumi resources.
"""

import dataclasses
import pytest
from typing import List
import unittest
from unittest import mock

from google.api_core import exceptions
import pulumi

from buildflow.io.providers.gcp import gcs_file_stream


class MyMocks(pulumi.runtime.Mocks):
    def new_resource(self, args: pulumi.runtime.MockResourceArgs):
        return [args.name + "_id", args.inputs]

    def call(self, args: pulumi.runtime.MockCallArgs):
        return {}


pulumi.runtime.set_mocks(MyMocks())


@dataclasses.dataclass
class _FakeBucket:
    name: str
    notifications: List[str]

    def list_notifications(self):
        return self.notifications


@dataclasses.dataclass
class FakeTopic:
    name: str


@pytest.mark.usefixtures("event_loop_instance")
class GCSFileStreamTest(unittest.TestCase):
    def get_async_result(self, coro):
        """Run a coroutine synchronously."""
        return self.event_loop.run_until_complete(coro)

    @mock.patch("google.cloud.storage.Client")
    @mock.patch("pulumi_gcp.storage.get_project_service_account")
    @pulumi.runtime.test
    def test_gcs_file_stream_pulumi_base(
        self, gcs_sa_mock: mock.MagicMock, gcs_client: mock.MagicMock
    ):
        want_project = "my-project"
        want_bucket = "my-bucket"

        gcs_client.return_value.get_bucket.side_effect = [
            exceptions.Forbidden("unused")
        ]
        provider = gcs_file_stream.GCSFileStreamProvider(
            bucket_name=want_bucket, project_id=want_project
        )
        pulumi_resources = provider.pulumi(type_=None)

        resources = pulumi_resources.resources
        # exports = pulumi_resources.exports

        self.assertEqual(len(resources), 5)

        # topic = resources[0]
        # binding = resources[1]
        # subscription = resources[2]
        # bucket = resources[3]
        # notification = resources[4]

        # def check_topic(args):
        #     _, name, project = args

        #     self.assertEqual(name, "my-bucket")
        #     self.assertEqual(project, want_project)
        #     assert False

        # pulumi.Output.all(topic.urn, topic.name, topic.project).apply(check_topic)


if __name__ == "__main__":
    unittest.main()
