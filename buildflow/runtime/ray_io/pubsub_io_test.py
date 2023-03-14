import unittest
from unittest import mock

from google.api_core import exceptions

from buildflow.runtime.ray_io import pubsub_io as io


class PubsubIOTest(unittest.TestCase):

    @mock.patch('google.cloud.pubsub.PublisherClient')
    @mock.patch('google.cloud.pubsub.SubscriberClient')
    def test_pubsub_source_setup_create_sub_and_topic(
        self,
        sub_client_mock: mock.MagicMock,
        pub_client_mock: mock.MagicMock,
    ):
        pub_mock = pub_client_mock.return_value
        sub_mock = sub_client_mock.return_value

        pub_mock.get_topic.side_effect = exceptions.NotFound('unused')
        sub_mock.get_subscription.side_effect = exceptions.NotFound('unused')

        pubsub_io = io.PubSubSource(topic='pubsub-topic',
                                    subscription='pubsub-sub')
        pubsub_io.setup()

        pub_mock.create_topic.assert_called_once_with(name='pubsub-topic')
        sub_mock.create_subscription.assert_called_once_with(
            name='pubsub-sub', topic='pubsub-topic', ack_deadline_seconds=600)

    @mock.patch('google.cloud.pubsub.PublisherClient')
    @mock.patch('google.cloud.pubsub.SubscriberClient')
    def test_pubsub_source_setup_create_only_sub(
        self,
        sub_client_mock: mock.MagicMock,
        pub_client_mock: mock.MagicMock,
    ):
        pub_mock = pub_client_mock.return_value
        sub_mock = sub_client_mock.return_value
        sub_mock.get_subscription.side_effect = exceptions.NotFound('unused')

        pubsub_io = io.PubSubSource(topic='pubsub-topic',
                                    subscription='pubsub-sub')
        pubsub_io.setup()

        pub_mock.create_topic.assert_not_called()
        sub_mock.create_subscription.assert_called_once_with(
            name='pubsub-sub', topic='pubsub-topic', ack_deadline_seconds=600)

    @mock.patch('google.cloud.pubsub.PublisherClient')
    @mock.patch('google.cloud.pubsub.SubscriberClient')
    def test_pubsub_source_setup_create_none_created(
        self,
        sub_client_mock: mock.MagicMock,
        pub_client_mock: mock.MagicMock,
    ):
        pub_mock = pub_client_mock.return_value
        sub_mock = sub_client_mock.return_value

        pubsub_io = io.PubSubSource(topic='pubsub-topic',
                                    subscription='pubsub-sub')
        pubsub_io.setup()

        pub_mock.create_topic.assert_not_called()
        sub_mock.create_subscription.assert_not_called()

    @mock.patch('google.cloud.pubsub.SubscriberClient')
    def test_pubsub__source_setup_create_sub_no_topic(
        self,
        sub_client_mock: mock.MagicMock,
    ):
        sub_mock = sub_client_mock.return_value
        sub_mock.get_subscription.side_effect = exceptions.NotFound('unused')

        pubsub_io = io.PubSubSource(topic='', subscription='pubsub-sub')
        with self.assertRaisesRegex(ValueError,
                                    'subscription: pubsub-sub was not found'):
            pubsub_io.setup()

    @mock.patch('google.cloud.pubsub.PublisherClient')
    def test_pubsub_setup_create_topic(
        self,
        pub_client_mock: mock.MagicMock,
    ):
        pub_mock = pub_client_mock.return_value

        pub_mock.get_topic.side_effect = exceptions.NotFound('unused')

        pubsub_io = io.PubSubSink('pubsub-topic')
        pubsub_io.setup(None)

        pub_mock.create_topic.assert_called_once_with(name='pubsub-topic')


if __name__ == '__main__':
    unittest.main()
