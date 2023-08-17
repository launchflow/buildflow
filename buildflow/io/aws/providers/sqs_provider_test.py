import unittest

import pulumi
import pulumi_aws

from buildflow.core.credentials.empty_credentials import EmptyCredentials
from buildflow.io.aws.providers import sqs_provider


class MyMocks(pulumi.runtime.Mocks):
    def new_resource(self, args: pulumi.runtime.MockResourceArgs):
        return [args.name + "_id", args.inputs]

    def call(self, args: pulumi.runtime.MockCallArgs):
        return {}


pulumi.runtime.set_mocks(
    MyMocks(),
    preview=False,
)


class SqsProviderTest(unittest.TestCase):
    @pulumi.runtime.test
    def test_pulumi_resources(self):
        provider = sqs_provider.SQSQueueProvider(
            queue_name="test_queue", aws_account_id="123456789", aws_region="us-east-1"
        )

        pulumi_resource = provider.pulumi_resource(
            type_=None,
            credentials=EmptyCredentials(None),
            opts=pulumi.ResourceOptions(),
        )

        child_resources = list(pulumi_resource._childResources)
        self.assertEqual(len(child_resources), 1)

        queue_resource = child_resources[0]

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
