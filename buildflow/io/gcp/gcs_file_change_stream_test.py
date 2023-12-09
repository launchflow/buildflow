import unittest
from unittest import mock

import pulumi
import pulumi_gcp

from buildflow.config.cloud_provider_config import GCPOptions
from buildflow.core.credentials.empty_credentials import EmptyCredentials
from buildflow.io.gcp.gcs_file_change_stream import GCSFileChangeStream
from buildflow.io.gcp.storage import GCSBucket


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

    @mock.patch("google.cloud.storage.Client")
    @mock.patch("pulumi_gcp.storage.get_project_service_account")
    @pulumi.runtime.test
    def test_gcs_file_stream_pulumi_all_mananged(
        self, gcs_sa_mock: mock.MagicMock, gcs_client: mock.MagicMock
    ):
        want_project = "my-project"
        want_bucket = "my-bucket"

        resource = GCSFileChangeStream(
            gcs_bucket=GCSBucket(
                project_id=want_project,
                bucket_name=want_bucket,
            ),
            event_types=[],
        )

        resources = resource.pulumi_resources(
            EmptyCredentials(), pulumi.ResourceOptions()
        )
        self.assertEqual(len(resources), 2)

        binding_resource = None
        notificaion_resource = None

        for resource in resources:
            if isinstance(resource, pulumi_gcp.storage.Notification):
                notificaion_resource = resource
            elif isinstance(resource, pulumi_gcp.pubsub.TopicIAMBinding):
                binding_resource = resource
        if binding_resource is None:
            raise ValueError("binding_resource not found")
        if notificaion_resource is None:
            raise ValueError("notificaion_resource not found")


if __name__ == "__main__":
    unittest.main()
