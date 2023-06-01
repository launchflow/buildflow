import dataclasses
import pytest
from typing import List
import unittest
from unittest import mock

from google.api_core import exceptions
from google.iam.v1 import iam_policy_pb2
from google.iam.v1 import policy_pb2

from buildflow.io.providers.gcp import gcs_file_stream


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

    @mock.patch("google.cloud.pubsub.PublisherClient")
    @mock.patch("google.cloud.pubsub.SubscriberClient")
    @mock.patch("google.cloud.storage.Client")
    @mock.patch("google.auth.default")
    def test_create_all_resources(
        self,
        auth_mock: mock.MagicMock,
        storage_client_mock: mock.MagicMock,
        sub_client_mock: mock.MagicMock,
        pub_client_mock: mock.MagicMock,
    ):
        auth_mock.return_value = (None, None)
        pub_mock = pub_client_mock.return_value
        sub_mock = sub_client_mock.return_value
        storage_mock = storage_client_mock.return_value

        pub_mock.get_topic.side_effect = exceptions.NotFound("unused")
        sub_mock.get_subscription.side_effect = exceptions.NotFound("unused")
        storage_mock.get_bucket.side_effect = exceptions.NotFound("unused")

        pub_mock.create_topic.return_value = FakeTopic("name")
        pub_mock.get_iam_policy.return_value = policy_pb2.Policy()

        storage_mock.create_bucket.return_value.project_number = "123"  # noqa: E501

        gcs_provider = gcs_file_stream.GCSFileStreamProvider(
            bucket_name="bucket", project_id="project", event_types=["A", "B"]
        )
        self.get_async_result(gcs_provider.setup())

        pub_mock.create_topic.assert_called_once_with(
            name="projects/project/topics/bucket_notifications"
        )
        expected_set_policy = iam_policy_pb2.SetIamPolicyRequest(
            resource="name",
            policy=policy_pb2.Policy(
                bindings=[
                    policy_pb2.Binding(
                        role="roles/pubsub.publisher",
                        members=[
                            "serviceAccount:service-123@gs-project-accounts.iam.gserviceaccount.com"  # noqa: E501
                        ],
                    )
                ]
            ),
        )
        pub_mock.set_iam_policy.assert_called_once_with(request=expected_set_policy)
        sub_mock.create_subscription.assert_called_once_with(
            name="projects/project/subscriptions/bucket_subscriber",
            topic="projects/project/topics/bucket_notifications",
            ack_deadline_seconds=600,
        )
        storage_mock.create_bucket.assert_called_once_with("bucket", project="project")

        notification_mock = (
            storage_mock.create_bucket.return_value.notification
        )  # noqa: E501
        notification_mock.assert_called_once_with(
            topic_name="bucket_notifications",
            topic_project="project",
            event_types=["A", "B"],
        )

    @mock.patch("google.cloud.pubsub.PublisherClient")
    @mock.patch("google.cloud.pubsub.SubscriberClient")
    @mock.patch("google.cloud.storage.Client")
    @mock.patch("google.auth.default")
    def test_existing_resources(
        self,
        auth_mock: mock.MagicMock,
        storage_client_mock: mock.MagicMock,
        sub_client_mock: mock.MagicMock,
        pub_client_mock: mock.MagicMock,
    ):
        auth_mock.return_value = (None, None)
        pub_mock = pub_client_mock.return_value
        sub_mock = sub_client_mock.return_value
        storage_mock = storage_client_mock.return_value

        storage_mock.create_bucket.return_value.project_number = "123"  # noqa: E501

        gcs_provider = gcs_file_stream.GCSFileStreamProvider(
            bucket_name="bucket",
            project_id="project",
            pubsub_subscription="projects/project/subscriptions/mysub",
            pubsub_topic="projects/project/topics/mytopic",
        )
        self.get_async_result(gcs_provider.setup())

        pub_mock.create_topic.assert_not_called()
        sub_mock.create_subscription.assert_not_called()
        storage_mock.create_bucket.assert_not_called()

        notification_mock = (
            storage_mock.create_bucket.return_value.notification
        )  # noqa: E501
        notification_mock.assert_not_called()

    def test_gcs_file_stream_plan_source(self):
        expected_plan = {
            "bucket_name": "bucket",
            "pubsub_topic": "projects/project/topics/bucket_notifications",
            "pubsub_subscription": ("projects/project/subscriptions/bucket_subscriber"),
        }
        gcs_provider = gcs_file_stream.GCSFileStreamProvider(
            bucket_name="bucket",
            project_id="project",
        )
        plan = self.get_async_result(gcs_provider.plan())
        self.assertEqual(expected_plan, plan)

    def test_gcs_file_stream_poll_converter_success(self):
        gcs_provider = gcs_file_stream.GCSFileStreamProvider(
            bucket_name="bucket",
            project_id="project",
        )
        gcs_provider.pull_converter(gcs_file_stream.GCSFileEvent)

    def test_gcs_file_stream_poll_converter_invalid_type(self):
        gcs_provider = gcs_file_stream.GCSFileStreamProvider(
            bucket_name="bucket",
            project_id="project",
        )
        with self.assertRaisesRegex(
            ValueError, "Input type for GCS file stream should be: `GCSFileEvent`"
        ):
            gcs_provider.pull_converter(str)


if __name__ == "__main__":
    unittest.main()
