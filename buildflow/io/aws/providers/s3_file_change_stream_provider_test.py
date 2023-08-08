import unittest

import pulumi
import pulumi_aws
import pytest

from buildflow.io.aws.providers.s3_file_change_stream_provider import (
    S3FileChangeStreamProvider,
)
from buildflow.io.aws.s3 import S3Bucket
from buildflow.io.aws.sqs import SQSQueue
from buildflow.types.aws import S3ChangeStreamEventType


@pytest.mark.skip(
    "gets mad about queue policy, and for some reason causes the sqs test to fail..."
)
class S3FileChangeStreamProviderTest(unittest.TestCase):
    def test_pulumi_resources(self):
        bucket_name = "test-bucket"
        queue_name = "queue-name"
        region = "us-east-1"
        bucket = S3Bucket(
            bucket_name=bucket_name, aws_region=region, file_format=None, file_path=None
        )
        sqs = SQSQueue(
            queue_name=queue_name, aws_region=region, aws_account_id=None
        ).options(managed=True)
        provider = S3FileChangeStreamProvider(
            s3_bucket=bucket,
            sqs_queue=sqs,
            event_types=[S3ChangeStreamEventType.OBJECT_CREATED_ALL],
        )

        pulumi_resource = provider.pulumi_resource(
            type_=None,
            credentials=None,
            opts=pulumi.ResourceOptions(),
        )

        resources = list(pulumi_resource._childResources)
        self.assertEqual(len(resources), 2)

        policy_resource = None
        notificaion_resource = None

        for resource in resources:
            if isinstance(resource, pulumi_aws.s3.BucketNotification):
                notificaion_resource = resource
            elif isinstance(resource, pulumi_aws.sqs.QueuePolicy):
                policy_resource = resource
            else:
                raise ValueError(f"Unexpected resource type: {type(resource)}")
        if policy_resource is None:
            raise ValueError("policy_resource not found")
        if notificaion_resource is None:
            raise ValueError("notificaion_resource not found")


if __name__ == "__main__":
    unittest.main()
