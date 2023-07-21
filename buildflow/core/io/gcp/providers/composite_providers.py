from typing import List, Optional, Type

from buildflow.core.credentials import GCPCredentials
from buildflow.core.io.gcp.providers.storage_providers import GCSBucketProvider
from buildflow.core.io.gcp.providers.pubsub_providers import (
    GCPPubSubSubscriptionProvider,
)
from buildflow.core.io.gcp.strategies.composite_strategies import GCSFileStreamSource
from buildflow.core.providers.provider import PulumiProvider, SourceProvider


class GCSFileStreamProvider(SourceProvider, PulumiProvider):
    def __init__(
        self,
        *,
        # NOTE: gcs_bucket_provider is only needed as a PulumiProvider, so its optional
        # for the case where we only want to use the source_provider
        gcs_bucket_provider: Optional[GCSBucketProvider],
        pubsub_subscription_provider: GCPPubSubSubscriptionProvider,
        # source-only options
        event_types: Optional[List[str]] = ("OBJECT_FINALIZE",),
        # pulumi-only options
        # TODO: Change this to True once we have a way to set this field
        destroy_protection: bool = False,
    ):
        self.gcs_bucket_provider = gcs_bucket_provider
        self.pubsub_subscription_provider = pubsub_subscription_provider
        # source-only options
        self.event_types = list(event_types)
        # pulumi-only options
        self.destroy_protection = destroy_protection

    def source(self, credentials: GCPCredentials):
        return GCSFileStreamSource(
            credentials=credentials,
            pubsub_source=self.pubsub_subscription_provider.source(),
        )

    def pulumi_resources(self, type_: Optional[Type]):
        if self.gcs_bucket_provider is None:
            raise ValueError(
                "Cannot create Pulumi resources for GCSFileStreamProvider without a "
                "GCSBucketProvider."
            )
        gcs_resources = self.gcs_bucket_provider.pulumi_resources(type_)
        pubsub_resources = self.pubsub_subscription_provider.pulumi_resources(type_)
        return gcs_resources + pubsub_resources
