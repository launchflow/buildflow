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

import pulumi
import pulumi_gcp


from buildflow.core.io.gcp.providers.composite_providers import GCSFileStreamProvider
from buildflow.core.io.gcp.providers.pubsub_providers import (
    GCPPubSubTopicProvider,
    GCPPubSubSubscriptionProvider,
)
from buildflow.core.io.gcp.providers.storage_providers import GCSBucketProvider


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

        provider = GCSFileStreamProvider(
            gcs_bucket_provider=GCSBucketProvider(
                project_id=want_project,
                bucket_name=want_bucket,
                bucket_region="us-central1",
            ),
            pubsub_topic_provider=GCPPubSubTopicProvider(
                project_id=want_project, topic_name="my-topic"
            ),
            pubsub_subscription_provider=GCPPubSubSubscriptionProvider(
                project_id=want_project,
                subscription_name="my-subscription",
                topic_id=f"projects/{want_project}/topics/my-topic",
            ),
            project_id=want_project,
        )

        resources = provider.pulumi_resources(type_=None)

        self.assertEqual(len(resources), 5)
        bucket = resources[0]
        topic = resources[1]
        subscription = resources[2]
        notification = resources[3]
        binding = resources[4]
        self.assertIsInstance(bucket.resource, pulumi_gcp.storage.Bucket)
        self.assertIsInstance(topic.resource, pulumi_gcp.pubsub.Topic)
        self.assertIsInstance(subscription.resource, pulumi_gcp.pubsub.Subscription)
        self.assertIsInstance(notification.resource, pulumi_gcp.storage.Notification)
        self.assertIsInstance(binding.resource, pulumi_gcp.pubsub.TopicIAMBinding)


if __name__ == "__main__":
    unittest.main()
