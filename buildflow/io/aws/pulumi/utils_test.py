import unittest

from buildflow.io.aws.pulumi.utils import arn_to_cloud_console_url


class ARNToCloudConsoleURLTests(unittest.TestCase):
    def test_s3_url(self):
        arn = "arn:aws:s3:::buildflow-testing"

        url = arn_to_cloud_console_url([arn])

        self.assertEqual(
            url,
            "https://s3.console.aws.amazon.com/s3/buckets/buildflow-testing",
        )

    def test_sqs_url(self):
        arn = "arn:aws:sqs:us-east-2:123456:buildflow-sqs"

        url = arn_to_cloud_console_url([arn])

        self.assertEqual(
            url,
            "https://us-east-2.console.aws.amazon.com/sqs/v2/home?region=us-east-2#/queues/https%3A%2F%2Fsqs.us-east-2.amazonaws.com%2F123456%2Fbuildflow-sqs",
        )


if __name__ == "__main__":
    unittest.main()
