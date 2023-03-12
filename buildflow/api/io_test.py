import unittest
from unittest import mock


class IoSetupTest(unittest.TestCase):

    @mock.patch('google.cloud.pubsub.PublisherClient')
    @mock.patch('google.cloud.pubsub.SubscriberClient')
    def test_pubsub_setup_create_sub_and_topic(
        self,
        sub_client_mock: mock.MagicMock,
        pub_client_mock: mock.MagicMock,
    ):
        pass

    @mock.patch('google.cloud.pubsub.PublisherClient')
    @mock.patch('google.cloud.pubsub.SubscriberClient')
    def test_pubsub_setup_create_only_sub(
        self,
        sub_client_mock: mock.MagicMock,
        pub_client_mock: mock.MagicMock,
    ):
        pass

    @mock.patch('google.cloud.pubsub.PublisherClient')
    @mock.patch('google.cloud.pubsub.SubscriberClient')
    def test_pubsub_setup_create_none_created(
        self,
        sub_client_mock: mock.MagicMock,
        pub_client_mock: mock.MagicMock,
    ):
        pass

    def test_bigquery_source_setup(self):
        pass

    def test_bigquery_sink_setup(self):
        pass


if __name__ == '__main__':
    unittest.main()
