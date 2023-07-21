import dataclasses

from buildflow.config.cloud_provider_config import GCPOptions
from buildflow.core.options.runtime_options import RuntimeOptions
from buildflow.core.io.gcp.storage import GCSBucket
from buildflow.core.io.gcp.providers.composite_providers import GCSFileStreamProvider
from buildflow.core.io.gcp.pubsub import GCPPubSubSubscription
from buildflow.core.io.primitive import GCPPrimtive


@dataclasses.dataclass
class GCSFileStream(GCPPrimtive):
    gcs_bucket: GCSBucket
    pubsub_subscription: GCPPubSubSubscription

    @classmethod
    def from_gcp_options(cls, gcp_options: GCPOptions) -> "GCSBucket":
        gcs_bucket = GCSBucket.from_gcp_options(gcp_options)
        pubsub_subscription = GCPPubSubSubscription.from_gcp_options(gcp_options)
        return cls(
            gcs_bucket=gcs_bucket,
            pubsub_subscription=pubsub_subscription,
        )

    def source_provider(self, runtime_options: RuntimeOptions):
        return GCSFileStreamProvider(
            runtime_options=runtime_options,
            gcs_bucket_provider=None,
            pubsub_subscription_provider=self.pubsub_subscription.source_provider(),
        )

    def pulumi_provider(self):
        return GCSFileStreamProvider(
            gcs_bucket_provider=self.gcs_bucket.pulumi_provider(),
            pubsub_subscription_provider=self.pubsub_subscription.pulumi_provider(),
        )
