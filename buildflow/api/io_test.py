from dataclasses import dataclass
import inspect
import unittest
from unittest import mock

from google.api_core import exceptions
from google.cloud import bigquery

from buildflow.api import io


class IoSetupTest(unittest.TestCase):

    @mock.patch('google.cloud.pubsub.PublisherClient')
    @mock.patch('google.cloud.pubsub.SubscriberClient')
    def test_pubsub_setup_create_sub_and_topic(
        self,
        sub_client_mock: mock.MagicMock,
        pub_client_mock: mock.MagicMock,
    ):
        pub_mock = pub_client_mock.return_value
        sub_mock = sub_client_mock.return_value

        pub_mock.get_topic.side_effect = exceptions.NotFound('unused')
        sub_mock.get_subscription.side_effect = exceptions.NotFound('unused')

        pubsub_io = io.PubSub('pubsub-topic', 'pubsub-sub')
        pubsub_io.setup(True, True, None)

        pub_mock.create_topic.assert_called_once_with(name='pubsub-topic')
        sub_mock.create_subscription.assert_called_once_with(
            name='pubsub-sub', topic='pubsub-topic')

    @mock.patch('google.cloud.pubsub.PublisherClient')
    @mock.patch('google.cloud.pubsub.SubscriberClient')
    def test_pubsub_setup_create_only_sub(
        self,
        sub_client_mock: mock.MagicMock,
        pub_client_mock: mock.MagicMock,
    ):
        pub_mock = pub_client_mock.return_value
        sub_mock = sub_client_mock.return_value
        sub_mock.get_subscription.side_effect = exceptions.NotFound('unused')

        pubsub_io = io.PubSub('pubsub-topic', 'pubsub-sub')
        pubsub_io.setup(True, True, None)

        pub_mock.create_topic.assert_not_called()
        sub_mock.create_subscription.assert_called_once_with(
            name='pubsub-sub', topic='pubsub-topic')

    @mock.patch('google.cloud.pubsub.PublisherClient')
    @mock.patch('google.cloud.pubsub.SubscriberClient')
    def test_pubsub_setup_create_none_created(
        self,
        sub_client_mock: mock.MagicMock,
        pub_client_mock: mock.MagicMock,
    ):
        pub_mock = pub_client_mock.return_value
        sub_mock = sub_client_mock.return_value

        pub_mock.get_topic.side_effect = exceptions.NotFound('unused')

        pubsub_io = io.PubSub('pubsub-topic', 'pubsub-sub')
        pubsub_io.setup(True, True, None)

        pub_mock.create_topic.assert_called_once_with(name='pubsub-topic')
        sub_mock.create_subscription.assert_not_called()

    @mock.patch('google.cloud.pubsub.SubscriberClient')
    def test_pubsub_setup_create_sub_no_topic(
        self,
        sub_client_mock: mock.MagicMock,
    ):
        sub_mock = sub_client_mock.return_value
        sub_mock.get_subscription.side_effect = exceptions.NotFound('unused')

        pubsub_io = io.PubSub('', 'pubsub-sub')
        with self.assertRaisesRegex(ValueError,
                                    'subscription: pubsub-sub was not found'):
            pubsub_io.setup(True, True, None)

    @mock.patch('google.cloud.bigquery.Client')
    def test_bigquery_source_setup_table_id(self, bq_mock: mock.MagicMock):
        bq = io.BigQuery(table_id='p.ds.t')

        bq.setup(source=True, sink=False, process_arg_spec=None)

        bq_mock.return_value.get_table.assert_called_once_with(table='p.ds.t')

    @mock.patch('google.cloud.bigquery.Client')
    def test_bigquery_source_setup_query(self, bq_mock: mock.MagicMock):
        bq = io.BigQuery(query='query')

        bq.setup(source=True, sink=False, process_arg_spec=None)

        bq_mock.return_value.query.assert_called_once()

    @mock.patch('google.cloud.bigquery.Client')
    def test_bigquery_sink_setup_create_table(self, bq_mock: mock.MagicMock):
        bq_client_mock = bq_mock.return_value
        bq_client_mock.get_table.side_effect = exceptions.NotFound('unused')

        bq = io.BigQuery(table_id='p.ds.t')

        @dataclass
        class Output:
            field: int

        def process() -> Output:
            pass

        bq.setup(source=False,
                 sink=True,
                 process_arg_spec=inspect.getfullargspec(process))

        bq_client_mock.create_dataset.assert_called_once_with('p.ds',
                                                              exists_ok=True)

        bq_client_mock.create_table.assert_called_once()

        table_call: bigquery.Table = bq_client_mock.create_table.call_args[0][
            0]
        self.assertEqual(table_call.project, 'p')
        self.assertEqual(table_call.dataset_id, 'ds')
        self.assertEqual(table_call.table_id, 't')
        self.assertEqual(table_call.schema, [
            bigquery.SchemaField(
                name='field', field_type='INTEGER', mode='REQUIRED')
        ])

    @mock.patch('google.cloud.bigquery.Client')
    def test_bigquery_sink_setup_schema_mismatch(self,
                                                 bq_mock: mock.MagicMock):
        bq_client_mock = bq_mock.return_value
        bq_client_mock.get_table.return_value = bigquery.Table(
            'p.ds.j',
            schema=[
                bigquery.SchemaField(name='field',
                                     field_type='FLOAT',
                                     mode='REQUIRED')
            ])

        bq = io.BigQuery(table_id='p.ds.t')

        @dataclass
        class Output:
            field: int

        def process() -> Output:
            pass

        with self.assertRaises(ValueError):
            bq.setup(source=False,
                     sink=True,
                     process_arg_spec=inspect.getfullargspec(process))

    @mock.patch('google.cloud.bigquery.Client')
    def test_bigquery_sink_setup_schema_match(self, bq_mock: mock.MagicMock):
        bq_client_mock = bq_mock.return_value
        bq_client_mock.get_table.return_value = bigquery.Table(
            'p.ds.j',
            schema=[
                bigquery.SchemaField(name='field',
                                     field_type='INTEGER',
                                     mode='REQUIRED')
            ])

        bq = io.BigQuery(table_id='p.ds.t')

        @dataclass
        class Output:
            field: int

        def process() -> Output:
            pass

        bq.setup(source=False,
                 sink=True,
                 process_arg_spec=inspect.getfullargspec(process))


if __name__ == '__main__':
    unittest.main()
