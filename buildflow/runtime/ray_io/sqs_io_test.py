import logging
import os
import shutil
import tempfile
import time
from typing import Any, Dict, List
import unittest
from unittest import mock

import boto3
from moto import mock_sqs
import pyarrow.parquet as pq
import pytest

import buildflow


class FakeSqsClient:

    def __init__(self, responses: List[Dict[str, Any]]) -> None:
        self.calls = 0
        self.responses = responses

    def receive_message(self, **kwargs):
        if self.calls >= len(self.responses):
            return {}
        to_ret = self.responses[self.calls]
        self.calls += 1
        return to_ret

    def get_queue_url(self, **kwargs):
        return {'QueueUrl': 'url'}

    def delete_message_batch(self, **kwargs):
        pass


class SqsIoTest(unittest.TestCase):

    def setUp(self) -> None:
        self.output_path = tempfile.mkdtemp()
        self.flow = buildflow.Flow()

    def tearDown(self) -> None:
        shutil.rmtree(self.output_path)

    @pytest.fixture(autouse=True)
    def inject_fixtures(self, caplog):
        self._caplog = caplog

    @mock_sqs
    def test_sqs_setup_create_queue(self):
        input_sqs = buildflow.SQSSource(queue_name='queue_name',
                                        region='us-east-2')

        with self._caplog.at_level(logging.WARNING):
            input_sqs.setup()
            self.assertEqual(self._caplog.records[0].message,
                             'Queue does not exist attempting to create')

        client = boto3.client('sqs', region_name='us-east-2')

        response = client.get_queue_url(QueueName='queue_name')

        self.assertEqual(
            response['QueueUrl'],
            'https://sqs.us-east-2.amazonaws.com/123456789012/queue_name')

    @mock_sqs
    def test_sqs_setup_queue_exists(self):
        input_sqs = buildflow.SQSSource(queue_name='queue_name',
                                        region='us-east-2')

        client = boto3.client('sqs', region_name='us-east-2')
        client.create_queue(QueueName='queue_name')

        with self._caplog.at_level(logging.WARNING):
            input_sqs.setup()
            self.assertEqual(len(self._caplog.records), 0)

    def test_sqs_source(self):
        path = os.path.join(self.output_path, 'output.parquet')

        fake_sqs = FakeSqsClient(responses=[{
            'Messages': [
                {
                    'MessageId': '1',
                    'ReceiptHandle': '2',
                    'Body': {
                        'field': 1
                    },
                },
                {
                    'MessageId': '3',
                    'ReceiptHandle': '4',
                    'Body': {
                        'field': 2
                    },
                },
            ]
        }])

        input_sqs = buildflow.SQSSource(queue_name='queue_name',
                                        region='us-east-2',
                                        _test_sqs_client=fake_sqs)

        @self.flow.processor(source=input_sqs,
                             sink=buildflow.FileSink(
                                 file_path=path,
                                 file_format=buildflow.FileFormat.PARQUET))
        def process(element):
            return element['Body']

        runner = self.flow.run()

        time.sleep(10)
        table = pq.read_table(path)
        self.assertEqual([{'field': 1}, {'field': 2}], table.to_pylist())

        runner.shutdown()

    @mock.patch('boto3.client')
    def test_sqs_source_disable_resource_creation(self,
                                                  boto_mock: mock.MagicMock):
        path = os.path.join(self.output_path, 'output.parquet')
        fake_sqs = FakeSqsClient(responses=[{
            'Messages': [
                {
                    'MessageId': '1',
                    'ReceiptHandle': '2',
                    'Body': {
                        'field': 1
                    },
                },
                {
                    'MessageId': '3',
                    'ReceiptHandle': '4',
                    'Body': {
                        'field': 2
                    },
                },
            ]
        }])

        input_sqs = buildflow.SQSSource(queue_name='queue_name',
                                        region='us-east-2',
                                        _test_sqs_client=fake_sqs)

        @self.flow.processor(source=input_sqs,
                             sink=buildflow.FileSink(
                                 file_path=path,
                                 file_format=buildflow.FileFormat.PARQUET))
        def process(element):
            return element['Body']

        runner = self.flow.run(enable_resource_creation=False)

        time.sleep(10)
        table = pq.read_table(path)
        self.assertEqual([{'field': 1}, {'field': 2}], table.to_pylist())

        runner.shutdown()

        # Should not be called because setup is not being called.
        boto_mock.assert_not_called()


if __name__ == '__main__':
    unittest.main()
