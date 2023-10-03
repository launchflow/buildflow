import unittest

import pulumi
import pulumi_aws

from buildflow.core.credentials.empty_credentials import EmptyCredentials
from buildflow.io.aws.sqs import SQSQueue


class MyMocks(pulumi.runtime.Mocks):
    def new_resource(self, args: pulumi.runtime.MockResourceArgs):
        return [args.name + "_id", args.inputs]

    def call(self, args: pulumi.runtime.MockCallArgs):
        return {}


pulumi.runtime.set_mocks(
    MyMocks(),
    preview=False,
)


class SQSResourceTest(unittest.TestCase):
    @pulumi.runtime.test
    def test_pulumi_resources(self):
        prim = SQSQueue(
            queue_name="test_queue",
            aws_account_id="123456789",
            aws_region="us-east-1",
        )

        resources = prim.pulumi_resources(EmptyCredentials(), pulumi.ResourceOptions())
        self.assertEqual(len(resources), 1)

        queue_resource = resources[0]

        self.assertIsInstance(queue_resource, pulumi_aws.sqs.Queue)

        def check_queue(args):
            _, name = args
            self.assertEqual(name, "test_queue")

        pulumi.Output.all(
            queue_resource.urn,
            queue_resource.name,
        ).apply(check_queue)


if __name__ == "__main__":
    unittest.main()
