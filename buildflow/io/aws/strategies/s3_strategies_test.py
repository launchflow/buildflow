import os
import unittest

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from moto import mock_s3

from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.core.options.credentials_options import CredentialsOptions
from buildflow.io.aws.strategies.s3_strategies import S3BucketSink
from buildflow.types.portable import FileFormat


class S3StrategiesTest(unittest.IsolatedAsyncioTestCase):
    async def test_s3_file_system(self):
        with mock_s3():
            os.environ["AWS_ACCESS_KEY_ID"] = "dummy"
            os.environ["AWS_SECRET_ACCESS_KEY"] = "dummy"

            self.bucket_name = "test-bucket"
            self.region = "us-east-1"
            s3_resource = boto3.resource("s3", region_name=self.region)
            self.s3_bucket = s3_resource.create_bucket(Bucket=self.bucket_name)
            self.creds = AWSCredentials(CredentialsOptions.default())
            self.file_path = "test.parquet"
            self.sink = S3BucketSink(
                credentials=self.creds,
                bucket_name=self.bucket_name,
                file_path=self.file_path,
                file_format=FileFormat.PARQUET,
            )
            await self.sink.push([{"a": 1}])
            objects = list(self.s3_bucket.objects.all())
            self.assertEqual(1, len(objects))
            object = objects[0]
            data = object.get()["Body"].read()
            table = pq.read_table(pa.BufferReader(data))

            self.assertEqual([{"a": 1}], table.to_pylist())


if __name__ == "__main__":
    unittest.main()
