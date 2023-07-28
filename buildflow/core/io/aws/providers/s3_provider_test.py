import unittest

import pulumi_aws

from buildflow.core.io.aws.providers.s3_provider import S3BucketProvider


class S3ProviderTest(unittest.TestCase):
    def test_pulumi_resources(self):
        bucket_name = "test-bucket"
        region = "us-east-1"
        provider = S3BucketProvider(bucket_name=bucket_name, aws_region=region)

        pulumi_resources = provider.pulumi_resources(type_=None, depends_on=[])

        self.assertEqual(len(pulumi_resources), 1)

        s3_resource = pulumi_resources[0]

        self.assertIsInstance(s3_resource.resource, pulumi_aws.s3.BucketV2)


if __name__ == "__main__":
    unittest.main()
