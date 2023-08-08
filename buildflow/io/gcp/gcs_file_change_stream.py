import dataclasses
from typing import Iterable

from buildflow.config.cloud_provider_config import GCPOptions
from buildflow.core.types.portable_types import BucketName
from buildflow.io.gcp.providers.gcs_file_change_stream import (
    GCSFileChangeStreamProvider,
)
from buildflow.io.gcp.pubsub_subscription import GCPPubSubSubscription
from buildflow.io.gcp.pubsub_topic import GCPPubSubTopic
from buildflow.io.gcp.storage import GCSBucket
from buildflow.io.primitive import CompositePrimitive, GCPPrimtive
from buildflow.types.gcp import GCSChangeStreamEventType


@dataclasses.dataclass
class GCSFileChangeStream(
    GCPPrimtive[
        # Pulumi provider type
        GCSFileChangeStreamProvider,
        # Source provider type
        GCSFileChangeStreamProvider,
        # Sink provider type
        None,
        # Background task provider type
        None,
    ],
    CompositePrimitive,
):
    gcs_bucket: GCSBucket
    event_types: Iterable[GCSChangeStreamEventType] = (
        GCSChangeStreamEventType.OBJECT_FINALIZE,
    )

    # Only needed for setting up resources.
    pubsub_subscription: GCPPubSubSubscription = dataclasses.field(init=False)

    def __post_init__(self):
        self.pubsub_subscription = GCPPubSubSubscription(
            project_id=self.gcs_bucket.project_id,
            subscription_name=f"{self.gcs_bucket.bucket_name}_subscription",
        ).options(
            managed=True,
            topic=GCPPubSubTopic(
                self.gcs_bucket.project_id,
                topic_name=f"{self.gcs_bucket.bucket_name}_topic",
            ).options(managed=True),
            include_attributes=True,
        )

    @classmethod
    def from_gcp_options(
        cls,
        gcp_options: GCPOptions,
        # TODO: Replace BucketName with GCSBucket
        bucket_name: BucketName,
        event_types: Iterable[GCSChangeStreamEventType],
    ) -> "GCSFileChangeStream":
        gcs_bucket = GCSBucket.from_gcp_options(
            gcp_options, bucket_name=bucket_name
        ).options(managed=True)
        return cls(gcs_bucket=gcs_bucket, event_types=event_types)

    def source_provider(self) -> GCSFileChangeStreamProvider:
        return GCSFileChangeStreamProvider(
            gcs_bucket=None,
            pubsub_subscription=self.pubsub_subscription,
            project_id=self.gcs_bucket.project_id,
            event_types=self.event_types,
        )

    def _pulumi_provider(self) -> GCSFileChangeStreamProvider:
        return GCSFileChangeStreamProvider(
            gcs_bucket=self.gcs_bucket,
            pubsub_subscription=self.pubsub_subscription,
            project_id=self.gcs_bucket.project_id,
            event_types=self.event_types,
        )
