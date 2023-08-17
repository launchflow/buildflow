import json
import os
import unittest

import boto3
import pytest
from moto import mock_sqs

from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.core.options.credentials_options import CredentialsOptions
from buildflow.core.types.aws_types import AWSRegion, SQSQueueName
from buildflow.io.aws.strategies.sqs_strategies import SQSSink, SQSSource


@pytest.mark.usefixtures("event_loop_instance")
class SqsStrategiesTest(unittest.TestCase):
    def get_async_result(self, coro):
        """Run a coroutine synchronously."""
        return self.event_loop.run_until_complete(coro)

    def _create_queue(self, queue_name: SQSQueueName, region: AWSRegion):
        self.sqs_client.create_queue(QueueName=queue_name)
        return self.sqs_client.get_queue_url(QueueName=queue_name)["QueueUrl"]

    def setUp(self) -> None:
        os.environ["AWS_ACCESS_KEY_ID"] = "dummy"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "dummy"
        self.region = "us-east-1"
        self.queue_name = "test_queue"
        self.sqs_client = boto3.client("sqs", region_name=self.region)
        self.creds = AWSCredentials(CredentialsOptions.default())

    @mock_sqs
    def test_sqs_sink_push(self):
        self.queue_url = self._create_queue(self.queue_name, self.region)
        sink = SQSSink(
            credentials=self.creds,
            queue_name=self.queue_name,
            aws_region=self.region,
            aws_account_id=None,
        )
        # Add more than ten elements to ensure we chunk if up properly
        self.get_async_result(sink.push([json.dumps({"a": 1})] * 12))

        response1 = self.sqs_client.receive_message(
            QueueUrl=self.queue_url,
            AttributeNames=["All"],
            MaxNumberOfMessages=10,
        )
        response2 = self.sqs_client.receive_message(
            QueueUrl=self.queue_url,
            AttributeNames=["All"],
            MaxNumberOfMessages=10,
        )
        messages = response1["Messages"] + response2["Messages"]
        self.assertEqual(len(messages), 12)

    @mock_sqs
    def test_sqs_source_pull(self):
        self.queue_url = self._create_queue(self.queue_name, self.region)
        sink = SQSSink(
            credentials=self.creds,
            queue_name=self.queue_name,
            aws_region=self.region,
            aws_account_id=None,
        )
        # push a bunch of elements to the queue
        self.get_async_result(sink.push([json.dumps({"a": 1})] * 12))

        source = SQSSource(
            credentials=self.creds,
            queue_name=self.queue_name,
            aws_region=self.region,
            aws_account_id=None,
        )

        backlog = self.get_async_result(source.backlog())
        self.assertEqual(backlog, 12)

        pull_response1 = self.get_async_result(source.pull())
        self.assertEqual(len(pull_response1.payload), 10)

        self.get_async_result(source.ack(pull_response1.ack_info, True))
        backlog = self.get_async_result(source.backlog())
        self.assertEqual(backlog, 2)

        pull_response2 = self.get_async_result(source.pull())
        self.assertEqual(len(pull_response2.payload), 2)
        self.get_async_result(source.ack(pull_response1.ack_info, True))
        backlog = self.get_async_result(source.backlog())
        self.assertEqual(backlog, 0)


if __name__ == "__main__":
    unittest.main()
