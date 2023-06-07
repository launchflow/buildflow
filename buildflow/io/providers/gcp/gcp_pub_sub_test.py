from dataclasses import asdict, dataclass
import json
import unittest
from unittest import mock

from google.api_core import exceptions
import pytest

from buildflow.io.providers.gcp import gcp_pub_sub


@pytest.mark.usefixtures("event_loop_instance")
class GCPPubsubTest(unittest.TestCase):
    def get_async_result(self, coro):
        """Run a coroutine synchronously."""
        return self.event_loop.run_until_complete(coro)

    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.pubsub.PublisherClient")
    @mock.patch("google.cloud.pubsub.SubscriberClient")
    def test_pubsub_source_setup_create_sub_and_topic(
        self,
        sub_client_mock: mock.MagicMock,
        pub_client_mock: mock.MagicMock,
        auth_mock: mock.MagicMock,
    ):
        auth_mock.return_value = (None, None)
        pub_mock = pub_client_mock.return_value
        sub_mock = sub_client_mock.return_value

        pub_mock.get_topic.side_effect = exceptions.NotFound("unused")
        sub_mock.get_subscription.side_effect = exceptions.NotFound("unused")

        pubsub_provider = gcp_pub_sub.GCPPubSubSubscriptionProvider(
            topic_id="projects/project/topics/pubsub-topic",
            subscription_name="pubsub-sub",
            batch_size=1000,
            include_attributes=False,
            project_id="project",
        )
        self.get_async_result(pubsub_provider.setup())

        pub_mock.create_topic.assert_called_once_with(
            name="projects/project/topics/pubsub-topic"
        )
        sub_mock.create_subscription.assert_called_once_with(
            name="projects/project/subscriptions/pubsub-sub",
            topic="projects/project/topics/pubsub-topic",
            ack_deadline_seconds=600,
        )

    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.pubsub.PublisherClient")
    @mock.patch("google.cloud.pubsub.SubscriberClient")
    def test_pubsub_source_setup_create_only_sub(
        self,
        sub_client_mock: mock.MagicMock,
        pub_client_mock: mock.MagicMock,
        auth_mock: mock.MagicMock,
    ):
        auth_mock.return_value = (None, None)
        pub_mock = pub_client_mock.return_value
        sub_mock = sub_client_mock.return_value
        sub_mock.get_subscription.side_effect = exceptions.NotFound("unused")

        pubsub_provider = gcp_pub_sub.GCPPubSubSubscriptionProvider(
            topic_id="projects/project/topics/pubsub-topic",
            subscription_name="pubsub-sub",
            batch_size=1000,
            include_attributes=False,
            project_id="project",
        )
        self.get_async_result(pubsub_provider.setup())

        pub_mock.create_topic.assert_not_called()
        sub_mock.create_subscription.assert_called_once_with(
            name="projects/project/subscriptions/pubsub-sub",
            topic="projects/project/topics/pubsub-topic",
            ack_deadline_seconds=600,
        )

    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.pubsub.PublisherClient")
    @mock.patch("google.cloud.pubsub.SubscriberClient")
    def test_pubsub_source_setup_create_none_created(
        self,
        sub_client_mock: mock.MagicMock,
        pub_client_mock: mock.MagicMock,
        auth_mock: mock.MagicMock,
    ):
        auth_mock.return_value = (None, None)
        pub_mock = pub_client_mock.return_value
        sub_mock = sub_client_mock.return_value

        pubsub_provider = gcp_pub_sub.GCPPubSubSubscriptionProvider(
            topic_id="projects/project/topics/pubsub-topic",
            subscription_name="pubsub-sub",
            batch_size=1000,
            include_attributes=False,
            project_id="project",
        )
        self.get_async_result(pubsub_provider.setup())

        pub_mock.create_topic.assert_not_called()
        sub_mock.create_subscription.assert_not_called()

    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.pubsub.SubscriberClient")
    def test_pubsub__source_setup_create_sub_no_topic(
        self,
        sub_client_mock: mock.MagicMock,
        auth_mock: mock.MagicMock,
    ):
        auth_mock.return_value = (None, None)
        sub_mock = sub_client_mock.return_value
        sub_mock.get_subscription.side_effect = exceptions.NotFound("unused")

        pubsub_provider = gcp_pub_sub.GCPPubSubSubscriptionProvider(
            topic_id="",
            subscription_name="pubsub-sub",
            batch_size=1000,
            include_attributes=False,
            project_id="project",
        )
        with self.assertRaisesRegex(
            ValueError,
            "subscription: projects/project/subscriptions/pubsub-sub was " "not found",
        ):
            self.get_async_result(pubsub_provider.setup())

    @pytest.mark.skip(reason="TODO: need to implement sink logic")
    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.pubsub.PublisherClient")
    def test_pubsub_setup_create_topic(
        self,
        pub_client_mock: mock.MagicMock,
        auth_mock: mock.MagicMock,
    ):
        pass
        # auth_mock.return_value = (None, None)
        # pub_mock = pub_client_mock.return_value

        # pub_mock.get_topic.side_effect = exceptions.NotFound("unused")

        # pubsub_io = io.GCPPubSubSink("projects/project/topics/pubsub-topic")
        # pubsub_io.setup(None)

        # pub_mock.create_topic.assert_called_once_with(
        #     name="projects/project/topics/pubsub-topic"
        # )

    def test_gcp_pubsub_plan_source(self):
        expected_plan = {
            "topic_id": "projects/other_project/topics/topic",
            "subscription_id": "projects/project/subscriptions/sub",
        }

        pubsub_provider = gcp_pub_sub.GCPPubSubSubscriptionProvider(
            topic_id="projects/other_project/topics/topic",
            subscription_name="sub",
            batch_size=1000,
            include_attributes=False,
            project_id="project",
        )
        plan = self.get_async_result(pubsub_provider.plan())

        self.assertEqual(expected_plan, plan)

    @pytest.mark.skip(reason="TODO: need to implement sink logic")
    def test_gcp_pubsub_plan_sink(self):
        pass
        # expected_plan = NodePlan(
        #     name="",
        #     processors=[
        #         ProcessorPlan(
        #             name="gcp_ps_process",
        #             source_resources={
        #                 "source_type": "GCPPubSubSource",
        #                 "subscription": "projects/p/subscriptions/sub",
        #             },
        #             sink_resources={
        #                 "sink_type": "GCPPubSubSink",
        #                 "topic": "projects/p/topics/topic2",
        #             },
        #         )
        #     ],
        # )
        # app = buildflow.Node()

        # @app.processor(
        #     source=io.GCPPubSubSource(
        #         subscription="projects/p/subscriptions/sub",
        #     ),
        #     sink=io.GCPPubSubSink(topic="projects/p/topics/topic2"),
        # )
        # def gcp_ps_process(elem):
        #     pass

        # plan = app.plan()
        # self.assertEqual(expected_plan, plan)

    def test_gcp_pubsub_poll_converter_bytes(self):
        pubsub_provider = gcp_pub_sub.GCPPubSubSubscriptionProvider(
            topic_id="projects/p/topics/topic",
            subscription_name="sub",
            batch_size=1000,
            include_attributes=False,
            project_id="project",
        )
        input_data = "test".encode("utf-8")
        converter = pubsub_provider.pull_converter(type(input_data))
        self.assertEqual(input_data, converter(input_data))

    def test_gcp_pubsub_poll_converter_dataclass(self):
        @dataclass
        class Test:
            a: int

        pubsub_provider = gcp_pub_sub.GCPPubSubSubscriptionProvider(
            topic_id="projects/p/topics/topic",
            subscription_name="sub",
            batch_size=1000,
            include_attributes=False,
            project_id="project",
        )
        input_data = Test(a=1)
        bytes_data = json.dumps(asdict(input_data)).encode("utf-8")
        converter = pubsub_provider.pull_converter(type(input_data))
        self.assertEqual(input_data, converter(bytes_data))

    def test_gcp_pubsub_poll_converter_none(self):
        pubsub_provider = gcp_pub_sub.GCPPubSubSubscriptionProvider(
            topic_id="projects/p/topics/topic",
            subscription_name="sub",
            batch_size=1000,
            include_attributes=False,
            project_id="project",
        )
        input_data = "test".encode("utf-8")
        converter = pubsub_provider.pull_converter(None)
        self.assertEqual(input_data, converter(input_data))

    def test_gcp_pubsub_push_converter_bytes(self):
        pubsub_provider = gcp_pub_sub.GCPPubSubSubscriptionProvider(
            topic_id="projects/p/topics/topic",
            subscription_name="sub",
            batch_size=1000,
            include_attributes=False,
            project_id="project",
        )
        input_data = "test".encode("utf-8")
        converter = pubsub_provider.push_converter(type(input_data))
        self.assertEqual(input_data, converter(input_data))

    def test_gcp_pubsub_push_converter_dataclass(self):
        @dataclass
        class Test:
            a: int

        pubsub_provider = gcp_pub_sub.GCPPubSubSubscriptionProvider(
            topic_id="projects/p/topics/topic",
            subscription_name="sub",
            batch_size=1000,
            include_attributes=False,
            project_id="project",
        )
        input_data = Test(a=1)
        bytes_data = json.dumps(asdict(input_data)).encode("utf-8")
        converter = pubsub_provider.push_converter(type(input_data))
        self.assertEqual(bytes_data, converter(input_data))

    def test_gcp_pubsub_push_converter_none(self):
        pubsub_provider = gcp_pub_sub.GCPPubSubSubscriptionProvider(
            topic_id="projects/p/topics/topic",
            subscription_name="sub",
            batch_size=1000,
            include_attributes=False,
            project_id="project",
        )
        input_data = "test".encode("utf-8")
        converter = pubsub_provider.push_converter(None)
        self.assertEqual(input_data, converter(input_data))


if __name__ == "__main__":
    unittest.main()
