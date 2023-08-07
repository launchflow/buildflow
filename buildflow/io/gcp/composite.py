import dataclasses
from typing import Iterable

from buildflow.config.cloud_provider_config import GCPOptions
from buildflow.core.io.gcp.providers.composite_providers import (
    GCSFileChangeStreamProvider,
)
from buildflow.core.io.primitive import CompositePrimitive, GCPPrimtive
from buildflow.core.types.portable_types import BucketName
from buildflow.io.gcp.pubsub import GCPPubSubSubscription, GCPPubSubTopic
from buildflow.io.gcp.storage import GCSBucket
from buildflow.types.gcp import GCSChangeStreamEventType


@dataclasses.dataclass
class GCSFileChangeStream(GCPPrimtive, CompositePrimitive):
    gcs_bucket: GCSBucket
    event_types: Iterable[GCSChangeStreamEventType] = (
        GCSChangeStreamEventType.OBJECT_FINALIZE,
    )

    # Only needed for setting up resources.
    pubsub_topic: GCPPubSubTopic = dataclasses.field(init=False)
    pubsub_subscription: GCPPubSubSubscription = dataclasses.field(init=False)

    def __post_init__(self):
        self.pubsub_topic = GCPPubSubTopic(
            self.gcs_bucket.project_id,
            topic_name=f"{self.gcs_bucket.bucket_name}_topic",
        ).options(managed=True)
        self.pubsub_subscription = GCPPubSubSubscription(
            project_id=self.gcs_bucket.project_id,
            topic_id=self.pubsub_topic.topic_id,
            subscription_name=f"{self.gcs_bucket.bucket_name}_subscription",
            include_attributes=True,
        ).options(managed=True)

    @classmethod
    def from_gcp_options(
        cls,
        gcp_options: GCPOptions,
        bucket_name: BucketName,
        event_types: Iterable[GCSChangeStreamEventType],
    ) -> "GCSFileChangeStream":
        gcs_bucket = GCSBucket.from_gcp_options(
            gcp_options, bucket_name=bucket_name
        ).options(managed=True)
        return cls(gcs_bucket=gcs_bucket, event_types=event_types)

    def source_provider(self) -> GCSFileChangeStreamProvider:
        return GCSFileChangeStreamProvider(
            gcs_bucket_provider=None,
            pubsub_topic_provider=None,
            pubsub_subscription_provider=self.pubsub_subscription.source_provider(),
            project_id=self.gcs_bucket.project_id,
            event_types=self.event_types,
        )

    def pulumi_provider(self) -> GCSFileChangeStreamProvider:
        return GCSFileChangeStreamProvider(
            gcs_bucket_provider=self.gcs_bucket.pulumi_provider(),
            pubsub_subscription_provider=self.pubsub_subscription.pulumi_provider(),
            pubsub_topic_provider=self.pubsub_topic.pulumi_provider(),
            project_id=self.gcs_bucket.project_id,
            event_types=self.event_types,
            subscription_managed=self.pubsub_subscription.managed,
            topic_managed=self.pubsub_topic.managed,
            bucket_managed=self.gcs_bucket.managed,
        )
