import logging
import os
import shutil
import tempfile
import time
from typing import Any, Dict, List
import unittest
from unittest import mock

import boto3
from moto import mock_sqs, mock_sts
import pyarrow.parquet as pq
import pytest

import buildflow
from buildflow.api import NodePlan, ProcessorPlan


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
        return {"QueueUrl": "url"}

    def delete_message_batch(self, **kwargs):
        pass


@pytest.mark.usefixtures("ray_fix")
@pytest.mark.usefixtures("event_loop_instance")
class SqsIoTest(unittest.TestCase):

    def setUp(self) -> None:
        self.output_path = tempfile.mkdtemp()
        self.app = buildflow.ComputeNode()

    def tearDown(self) -> None:
        shutil.rmtree(self.output_path)

    @pytest.fixture(autouse=True)
    def inject_fixtures(self, caplog):
        self._caplog = caplog

    def get_async_result(self, coro):
        """Run a coroutine synchronously."""
        return self.event_loop.run_until_complete(coro)

    @mock_sqs
    def test_sqs_setup_create_queue(self):
        input_sqs = buildflow.SQSSource(queue_name="queue_name",
                                        region="us-east-2")

        with self._caplog.at_level(logging.WARNING):
            input_sqs.setup()
            self.assertEqual(
                self._caplog.records[0].message,
                "Queue does not exist attempting to create",
            )

        client = boto3.client("sqs", region_name="us-east-2")

        response = client.get_queue_url(QueueName="queue_name")

        self.assertEqual(
            response["QueueUrl"],
            "https://sqs.us-east-2.amazonaws.com/123456789012/queue_name",
        )

    @mock_sqs
    def test_sqs_setup_queue_exists(self):
        input_sqs = buildflow.SQSSource(queue_name="queue_name",
                                        region="us-east-2")

        client = boto3.client("sqs", region_name="us-east-2")
        client.create_queue(QueueName="queue_name")

        with self._caplog.at_level(logging.WARNING):
            input_sqs.setup()
            self.assertEqual(len(self._caplog.records), 0)

    def test_sqs_source(self):
        path = os.path.join(self.output_path, "output.parquet")

        fake_sqs = FakeSqsClient(responses=[{
            "Messages": [
                {
                    "MessageId": "1",
                    "ReceiptHandle": "2",
                    "Body": {
                        "field": 1
                    },
                },
                {
                    "MessageId": "3",
                    "ReceiptHandle": "4",
                    "Body": {
                        "field": 2
                    },
                },
            ]
        }])

        input_sqs = buildflow.SQSSource(queue_name="queue_name",
                                        region="us-east-2",
                                        _test_sqs_client=fake_sqs)

        @self.app.processor(
            source=input_sqs,
            sink=buildflow.FileSink(file_path=path,
                                    file_format=buildflow.FileFormat.PARQUET),
        )
        def process(element):
            return element["Body"]

        runner = self.app.run(blocking=False)

        time.sleep(10)
        table = pq.read_table(path)
        self.assertEqual([{"field": 1}, {"field": 2}], table.to_pylist())

        self.get_async_result(runner.drain())

    @mock.patch("boto3.client")
    def test_sqs_source_disable_resource_creation(self,
                                                  boto_mock: mock.MagicMock):
        path = os.path.join(self.output_path, "output.parquet")
        fake_sqs = FakeSqsClient(responses=[{
            "Messages": [
                {
                    "MessageId": "1",
                    "ReceiptHandle": "2",
                    "Body": {
                        "field": 1
                    },
                },
                {
                    "MessageId": "3",
                    "ReceiptHandle": "4",
                    "Body": {
                        "field": 2
                    },
                },
            ]
        }])

        input_sqs = buildflow.SQSSource(queue_name="queue_name",
                                        region="us-east-2",
                                        _test_sqs_client=fake_sqs)

        @self.app.processor(
            source=input_sqs,
            sink=buildflow.FileSink(file_path=path,
                                    file_format=buildflow.FileFormat.PARQUET),
        )
        def process(element):
            return element["Body"]

        runner = self.app.run(disable_resource_creation=True, blocking=False)

        time.sleep(10)
        table = pq.read_table(path)
        self.assertEqual([{"field": 1}, {"field": 2}], table.to_pylist())

        self.get_async_result(runner.drain())

        # Should not be called because setup is not being called.
        boto_mock.assert_not_called()

    @mock_sqs
    @mock_sts
    def test_sqs_source_plan(self):
        os.environ["AWS_DEFAULT_REGION"] = "us-east-9"
        expected_plan = NodePlan(
            name="",
            processors=[
                ProcessorPlan(
                    name="sqs_process",
                    source_resources={
                        "source_type":
                        "SQSSource",
                        "queue_url":
                        "https://sqs.us-east-9.amazonaws.com/123456789012/queue_name",  # noqa: E501
                    },
                    sink_resources=None,
                )
            ],
        )
        app = buildflow.ComputeNode()

        @app.processor(source=buildflow.SQSSource(queue_name="queue_name"))
        def sqs_process(elem):
            pass

        plan = app.plan()
        self.assertEqual(expected_plan, plan)


if __name__ == "__main__":
    unittest.main()
