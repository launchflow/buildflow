import json
import os
import unittest

import boto3
import pytest
from moto import mock_sqs

from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.core.options.credentials_options import CredentialsOptions
from buildflow.core.types.aws_types import AWSRegion, SQSQueueName
from buildflow.io.aws.strategies.s3_file_change_stream_strategies import (
    S3FileChangeStreamSource,
)
from buildflow.io.aws.strategies.sqs_strategies import SQSSink, SQSSource
from buildflow.types.aws import S3ChangeStreamEventType


@pytest.mark.usefixtures("event_loop_instance")
class S3FileChangeStreamTest(unittest.TestCase):
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
    def test_s3_file_change_stream_test_event(self):
        self.queue_url = self._create_queue(self.queue_name, self.region)
        sink = SQSSink(
            credentials=self.creds,
            queue_name=self.queue_name,
            aws_region=self.region,
            aws_account_id=None,
        )
        # push a bunch of elements to the queue
        contents = {
            "Service": "Amazon S3",
            "Event": "s3:TestEvent",
            "Time": "2023-07-28T16:34:05.246Z",
            "Bucket": "test-bucket",
            "RequestId": "request-id",
            "HostId": "host-id",
        }
        self.get_async_result(
            sink.push(
                [
                    json.dumps(
                        {
                            "Service": "Amazon S3",
                            "Event": "s3:TestEvent",
                            "Time": "2023-07-28T16:34:05.246Z",
                            "Bucket": "test-bucket",
                            "RequestId": "request-id",
                            "HostId": "host-id",
                        }
                    )
                ]
            )
        )

        source = SQSSource(
            credentials=self.creds,
            queue_name=self.queue_name,
            aws_region=self.region,
            aws_account_id=None,
        )
        s3_stream = S3FileChangeStreamSource(
            sqs_source=source, aws_region=self.region, credentials=self.creds
        )

        backlog = self.get_async_result(s3_stream.backlog())
        self.assertEqual(backlog, 1)

        pull_response = self.get_async_result(s3_stream.pull())
        self.assertEqual(len(pull_response.payload), 1)

        payload = pull_response.payload[0]
        self.assertEqual(payload.bucket_name, "test-bucket")
        self.assertEqual(payload.event_type, S3ChangeStreamEventType.UNKNOWN)
        self.assertEqual(payload.metadata, contents)
        self.assertEqual(payload.file_path, None)

    @mock_sqs
    def test_s3_file_change_stream_create(self):
        self.queue_url = self._create_queue(self.queue_name, self.region)
        sink = SQSSink(
            credentials=self.creds,
            queue_name=self.queue_name,
            aws_region=self.region,
            aws_account_id=None,
        )
        want_create_path = "newfile.txt"
        want_delete_path = "newfile.txt"
        want_bucket = "test-bucket"
        contents = {
            "Records": [
                {
                    "s3": {
                        "object": {"key": want_create_path},
                        "bucket": {"name": want_bucket},
                    },
                    "eventName": "ObjectCreated:Put",
                },
                {
                    "s3": {
                        "object": {"key": want_delete_path},
                        "bucket": {"name": want_bucket},
                    },
                    "eventName": "ObjectRemoved:Delete",
                },
            ]
        }
        self.get_async_result(sink.push([json.dumps(contents)]))

        source = SQSSource(
            credentials=self.creds,
            queue_name=self.queue_name,
            aws_region=self.region,
            aws_account_id=None,
        )
        s3_stream = S3FileChangeStreamSource(
            sqs_source=source, aws_region=self.region, credentials=self.creds
        )

        backlog = self.get_async_result(s3_stream.backlog())
        self.assertEqual(backlog, 1)

        pull_response = self.get_async_result(s3_stream.pull())
        self.assertEqual(len(pull_response.payload), 2)

        create = pull_response.payload[0]
        self.assertEqual(create.bucket_name, "test-bucket")
        self.assertEqual(create.event_type, S3ChangeStreamEventType.OBJECT_CREATED_PUT)
        self.assertEqual(create.metadata, contents["Records"][0])
        self.assertEqual(create.file_path, want_create_path)

        delete = pull_response.payload[1]
        self.assertEqual(delete.bucket_name, "test-bucket")
        self.assertEqual(
            delete.event_type, S3ChangeStreamEventType.OBJECT_REMOVED_DELETE
        )
        self.assertEqual(delete.metadata, contents["Records"][1])
        self.assertEqual(delete.file_path, want_delete_path)


if __name__ == "__main__":
    unittest.main()
