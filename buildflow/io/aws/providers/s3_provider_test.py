import unittest

import pulumi
import pulumi_aws

from buildflow.core.credentials.empty_credentials import EmptyCredentials
from buildflow.io.aws.providers.s3_provider import S3BucketProvider
from buildflow.types.portable import FileFormat


class S3ProviderTest(unittest.TestCase):
    def test_pulumi_resources(self):
        bucket_name = "test-bucket"
        region = "us-east-1"
        provider = S3BucketProvider(
            bucket_name=bucket_name,
            aws_region=region,
            file_format=FileFormat.JSON,
            file_path="unused",
        )

        pulumi_resource = provider.pulumi_resource(
            type_=None,
            credentials=EmptyCredentials(None),
            opts=pulumi.ResourceOptions(),
        )

        child_resources = list(pulumi_resource._childResources)
        self.assertEqual(len(child_resources), 1)

        s3_resource = child_resources[0]

        self.assertIsInstance(s3_resource, pulumi_aws.s3.BucketV2)


if __name__ == "__main__":
    unittest.main()
