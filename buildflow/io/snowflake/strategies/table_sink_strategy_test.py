import unittest

import boto3
import pytest
from moto import mock_s3

from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.core.options.credentials_options import CredentialsOptions
from buildflow.io.aws.strategies.s3_strategies import S3BucketSink
from buildflow.io.snowflake.strategies.table_sink_startegy import SnowflakeTableSink
from buildflow.types.portable import FileFormat


@pytest.mark.usefixtures("event_loop_instance")
@mock_s3
class TableSinkStrategiesTest(unittest.TestCase):
    def get_async_result(self, coro):
        """Run a coroutine synchronously."""
        return self.event_loop.run_until_complete(coro)

    def setUp(self) -> None:
        self.creds = AWSCredentials(CredentialsOptions.default())
        self.bucket_name = "test-bucket"
        self.region = "us-east-1"
        s3_resource = boto3.resource("s3", region_name=self.region)
        self.s3_bucket = s3_resource.create_bucket(Bucket=self.bucket_name)
        self.file_path = "test.parquet"
        bucket_sink = S3BucketSink(
            credentials=self.creds,
            bucket_name=self.bucket_name,
            file_path=self.file_path,
            file_format=FileFormat.PARQUET,
        )

        self.sink = SnowflakeTableSink(
            credentials=self.creds,
            bucket_sink=bucket_sink,
        )

    def test_write_to_files(self):
        self.get_async_result(self.sink.push([{"a": 1}]))
        self.get_async_result(self.sink.push([{"a": 1}]))

        objects = []
        for obj in self.s3_bucket.objects.all():
            objects.append(obj.key)

        self.assertEqual(2, len(objects))
        self.assertTrue(objects[0].startswith("buildflow-staging"))
        self.assertTrue(objects[1].startswith("buildflow-staging"))


if __name__ == "__main__":
    unittest.main()
