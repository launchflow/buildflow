import unittest

from buildflow.config.cloud_provider_config import GCPOptions
from buildflow.io.gcp.gcs_file_change_stream import GCSFileChangeStream


class GCSFileChangeStreamTest(unittest.TestCase):
    def test_from_gcp_options(self):
        options = GCPOptions(
            default_project_id="pid",
            default_region="central",
            default_zone="central1-a",
        )
        stream = GCSFileChangeStream.from_gcp_options(
            options, "my-bucket", event_types=[]
        )
        self.assertEqual(stream.gcs_bucket.bucket_name, "my-bucket")
        self.assertEqual(
            stream.pubsub_subscription.subscription_name, "my-bucket_subscription"
        )


if __name__ == "__main__":
    unittest.main()
