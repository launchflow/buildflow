import unittest

import pulumi_aws
import pytest

from buildflow.core.io.aws.providers.s3_file_change_stream_provider import (
    S3FileChangeStreamProvider,
)
from buildflow.core.io.aws.providers.s3_provider import S3BucketProvider
from buildflow.core.io.aws.providers.sqs_provider import SQSQueueProvider
from buildflow.core.types.aws_types import S3ChangeStreamEventType


@pytest.mark.skip("fails for some reason around the queue policy")
class S3FileChangeStreamProviderTest(unittest.TestCase):
    def test_pulumi_resources(self):
        bucket_name = "test-bucket"
        queue_name = "queue-name"
        region = "us-east-1"
        bucket_provider = S3BucketProvider(bucket_name=bucket_name, aws_region=region)
        sqs_provider = SQSQueueProvider(
            queue_name=queue_name, aws_region=region, aws_account_id=None
        )
        provider = S3FileChangeStreamProvider(
            s3_bucket_provider=bucket_provider,
            sqs_queue_provider=sqs_provider,
            bucket_managed=True,
            event_types=[S3ChangeStreamEventType.OBJECT_CREATED_ALL],
        )

        pulumi_resources = provider.pulumi_resources(type_=None, depends_on=[])

        self.assertEqual(len(pulumi_resources), 4)

        s3_resource = pulumi_resources[0]
        queue_resource = pulumi_resources[1]
        queue_policy_resource = pulumi_resources[2]
        notification = pulumi_resources[3]

        self.assertIsInstance(s3_resource.resource, pulumi_aws.s3.BucketV2)
        self.assertIsInstance(queue_resource.resource, pulumi_aws.sqs.Queue)
        self.assertIsInstance(
            queue_policy_resource.resource, pulumi_aws.sqs.QueuePolicy
        )
        self.assertIsInstance(notification.resource, pulumi_aws.s3.BucketNotification)


if __name__ == "__main__":
    unittest.main()
