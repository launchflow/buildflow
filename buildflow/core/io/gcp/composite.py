import dataclasses
from typing import Iterable, Optional

from buildflow.config.cloud_provider_config import GCPOptions
from buildflow.core.io.gcp.providers.composite_providers import (
    GCSFileChangeStreamProvider,
)
from buildflow.core.io.gcp.pubsub import GCPPubSubSubscription, GCPPubSubTopic
from buildflow.core.io.gcp.storage import GCSBucket
from buildflow.core.io.primitive import CompositePrimitive, GCPPrimtive
from buildflow.core.types.gcp_types import GCSChangeStreamEventType
from buildflow.core.types.portable_types import BucketName


@dataclasses.dataclass
class GCSFileChangeStream(GCPPrimtive, CompositePrimitive):
    gcs_bucket: GCSBucket
    pubsub_subscription: GCPPubSubSubscription
    event_types: Iterable[GCSChangeStreamEventType] = (
        GCSChangeStreamEventType.OBJECT_FINALIZE,
    )

    # Only needed for setting up resources.
    pubsub_topic: Optional[GCPPubSubTopic] = None

    def __post_init__(self):
        self.pubsub_subscription.include_attributes = True

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
        topic_name = f"{bucket_name}_topic"
        pubsub_topic = GCPPubSubTopic.from_gcp_options(
            gcp_options, topic_name=topic_name
        ).options(managed=True)
        pubsub_subscription = GCPPubSubSubscription.from_gcp_options(
            gcp_options,
            topic_id=pubsub_topic.topic_id,
            subscription_name=f"{bucket_name}_subscription",
        ).options(managed=True)
        return cls(
            gcs_bucket=gcs_bucket,
            pubsub_subscription=pubsub_subscription,
            pubsub_topic=pubsub_topic,
            event_types=event_types,
        )

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
