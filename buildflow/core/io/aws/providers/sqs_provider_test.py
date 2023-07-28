import unittest

import pulumi
import pulumi_aws

from buildflow.core.io.aws.providers import sqs_provider


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

        pulumi_resources = provider.pulumi_resources(type_=None, depends_on=[])

        self.assertEqual(len(pulumi_resources), 1)

        queue_resource = pulumi_resources[0]

        self.assertIsInstance(queue_resource.resource, pulumi_aws.sqs.Queue)
        self.assertEqual(
            queue_resource.exports, {"aws.sqs.name.test_queue": "us-east-1-test_queue"}
        )

        def check_queue(args):
            _, name = args
            self.assertEqual(name, "test_queue")

        pulumi.Output.all(
            queue_resource.resource.urn,
            queue_resource.resource.name,
        ).apply(check_queue)


if __name__ == "__main__":
    unittest.main()
