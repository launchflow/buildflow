import unittest

import pulumi
import pulumi_aws

from buildflow.core.credentials.empty_credentials import EmptyCredentials
from buildflow.io.aws import S3Bucket


class MyMocks(pulumi.runtime.Mocks):
    def new_resource(self, args: pulumi.runtime.MockResourceArgs):
        return [args.name + "_id", args.inputs]

    def call(self, args: pulumi.runtime.MockCallArgs):
        return {}


pulumi.runtime.set_mocks(
    MyMocks(),
    preview=False,
)


class S3Test(unittest.TestCase):
    @pulumi.runtime.test
    def test_pulumi_resources(self):
        bucket_name = "test-bucket"
        region = "us-east-1"
        prim = S3Bucket(
            bucket_name=bucket_name,
            aws_region=region,
        ).options(force_destroy=True)
        resources = prim.pulumi_resources(EmptyCredentials(), pulumi.ResourceOptions())

        self.assertEqual(len(resources), 1)

        s3_resource = resources[0]

        self.assertIsInstance(s3_resource, pulumi_aws.s3.BucketV2)


if __name__ == "__main__":
    unittest.main()
