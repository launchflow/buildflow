import dataclasses
from typing import Iterable

import pulumi

from buildflow.config.cloud_provider_config import GCPOptions
from buildflow.core.credentials.gcp_credentials import GCPCredentials
from buildflow.core.types.portable_types import BucketName
from buildflow.io.gcp.pubsub_subscription import GCPPubSubSubscription
from buildflow.io.gcp.pubsub_topic import GCPPubSubTopic
from buildflow.io.gcp.pulumi.gcs_file_change_stream import (
    GCSFileChangeStreamPulumiResource,
)
from buildflow.io.gcp.storage import GCSBucket
from buildflow.io.gcp.strategies.gcs_file_change_stream_strategies import (
    GCSFileChangeStreamSource,
)
from buildflow.io.strategies.source import SourceStrategy
from buildflow.types.gcp import GCSChangeStreamEventType


@dataclasses.dataclass
class GCSFileChangeStream:
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
            topic=GCPPubSubTopic(
                self.gcs_bucket.project_id,
                topic_name=f"{self.gcs_bucket.bucket_name}_topic",
            ),
            include_attributes=True,
        )
        self.pubsub_subscription.enable_managed()

    @classmethod
    def from_gcp_options(
        cls,
        gcp_options: GCPOptions,
        # TODO: Replace BucketName with GCSBucket
        bucket_name: BucketName,
        event_types: Iterable[GCSChangeStreamEventType],
    ) -> "GCSFileChangeStream":
        gcs_bucket = GCSBucket.from_gcp_options(gcp_options, bucket_name=bucket_name)
        gcs_bucket.enable_managed()
        return cls(gcs_bucket=gcs_bucket, event_types=event_types)

    def source(self, credentials: GCPCredentials) -> SourceStrategy:
        return GCSFileChangeStreamSource(
            credentials=credentials,
            project_id=self.gcs_bucket.project_id,
            pubsub_source=self.pubsub_subscription.source(credentials=credentials),
        )

    def pulumi_resource(
        self, credentials: GCPCredentials, opts: pulumi.ResourceOptions
    ) -> GCSFileChangeStreamPulumiResource:
        return GCSFileChangeStreamPulumiResource(
            bucket=self.gcs_bucket,
            subscription=self.pubsub_subscription,
            event_types=self.event_types,
            credentials=credentials,
            opts=opts,
        )
