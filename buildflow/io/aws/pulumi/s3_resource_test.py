import unittest

import pulumi
import pulumi_aws

from buildflow.core.credentials.empty_credentials import EmptyCredentials
from buildflow.io.aws.pulumi.s3_resource import S3BucketResource


class S3ResourceTest(unittest.TestCase):
    def test_pulumi_resources(self):
        bucket_name = "test-bucket"
        region = "us-east-1"
        pulumi_resource = S3BucketResource(
            bucket_name=bucket_name,
            aws_region=region,
            force_destroy=False,
            credentials=EmptyCredentials(),
            opts=pulumi.ResourceOptions(),
        )

        child_resources = list(pulumi_resource._childResources)
        self.assertEqual(len(child_resources), 1)

        s3_resource = child_resources[0]

        self.assertIsInstance(s3_resource, pulumi_aws.s3.BucketV2)


if __name__ == "__main__":
    unittest.main()
