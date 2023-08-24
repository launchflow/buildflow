"""Tests for gcs_file_stream.py

TODO: these tests don't actually validate the pulumi resources unfortunately.

The `@pulumi.runtime.test` annotation only awaits what is returned so need
to refactor this. It's still worth keeping the tests though cause they
do test the basics of pulumi resources.
"""

import unittest
from unittest import mock

import pulumi
import pulumi_gcp
import pytest

from buildflow.core.credentials.empty_credentials import EmptyCredentials
from buildflow.io.gcp.providers.gcs_file_change_stream import (
    GCSFileChangeStreamProvider,
)
from buildflow.io.gcp.pubsub_subscription import GCPPubSubSubscription
from buildflow.io.gcp.pubsub_topic import GCPPubSubTopic
from buildflow.io.gcp.storage import GCSBucket


@pytest.mark.usefixtures("event_loop_instance")
class GCSFileChangeStreamTest(unittest.TestCase):
    def get_async_result(self, coro):
        """Run a coroutine synchronously."""
        return self.event_loop.run_until_complete(coro)

    @mock.patch("google.cloud.storage.Client")
    @mock.patch("pulumi_gcp.storage.get_project_service_account")
    @pulumi.runtime.test
    def test_gcs_file_stream_pulumi_all_mananged(
        self, gcs_sa_mock: mock.MagicMock, gcs_client: mock.MagicMock
    ):
        want_project = "my-project"
        want_bucket = "my-bucket"

        provider = GCSFileChangeStreamProvider(
            gcs_bucket=GCSBucket(
                project_id=want_project,
                bucket_name=want_bucket,
            ).options(managed=True),
            pubsub_subscription=GCPPubSubSubscription(
                project_id=want_project,
                subscription_name="my-subscription",
            ).options(
                managed=True,
                topic=GCPPubSubTopic(
                    topic_name="my-topic", project_id=want_project
                ).options(managed=True),
            ),
            project_id=want_project,
            event_types=[],
        )

        resource = provider.pulumi_resource(
            type_=None,
            credentials=EmptyCredentials(None),
            opts=pulumi.ResourceOptions(),
        )

        resources = list(resource._childResources)
        self.assertEqual(len(resources), 2)

        binding_resource = None
        notificaion_resource = None

        for resource in resources:
            if isinstance(resource, pulumi_gcp.storage.Notification):
                notificaion_resource = resource
            elif isinstance(resource, pulumi_gcp.pubsub.TopicIAMBinding):
                binding_resource = resource
        if binding_resource is None:
            raise ValueError("binding_resource not found")
        if notificaion_resource is None:
            raise ValueError("notificaion_resource not found")


if __name__ == "__main__":
    unittest.main()
