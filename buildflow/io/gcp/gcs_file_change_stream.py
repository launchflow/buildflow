import dataclasses
from typing import Iterable, List

import pulumi
import pulumi_gcp

from buildflow.config.cloud_provider_config import GCPOptions
from buildflow.core.credentials.gcp_credentials import GCPCredentials
from buildflow.core.types.portable_types import BucketName
from buildflow.io.gcp.pubsub_subscription import GCPPubSubSubscription
from buildflow.io.gcp.pubsub_topic import GCPPubSubTopic
from buildflow.io.gcp.storage import GCSBucket
from buildflow.io.gcp.strategies.gcs_file_change_stream_strategies import (
    GCSFileChangeStreamSource,
)
from buildflow.io.primitive import GCPPrimtive
from buildflow.io.strategies.source import SourceStrategy
from buildflow.types.gcp import GCSChangeStreamEventType


@dataclasses.dataclass
class GCSFileChangeStream(GCPPrimtive):
    gcs_bucket: GCSBucket
    event_types: Iterable[GCSChangeStreamEventType] = (
        GCSChangeStreamEventType.OBJECT_FINALIZE,
    )

    # Only needed for setting up resources.
    pubsub_subscription: GCPPubSubSubscription = dataclasses.field(init=False)
    pubsub_subscription: GCPPubSubTopic = dataclasses.field(init=False)

    def __post_init__(self):
        self.pubsub_topic = GCPPubSubTopic(
            self.gcs_bucket.project_id,
            topic_name=f"{self.gcs_bucket.bucket_name}_topic",
        )
        self.pubsub_topic.enable_managed()
        self.pubsub_subscription = GCPPubSubSubscription(
            project_id=self.gcs_bucket.project_id,
            subscription_name=f"{self.gcs_bucket.bucket_name}_subscription",
        ).options(topic=self.pubsub_topic, include_attributes=True)
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

    def primitive_id(self):
        return (
            f"{self.gcs_bucket.bucket_name}.{self.pubsub_subscription.topic.topic_name}"
        )

    def pulumi_resources(
        self, credentials: GCPCredentials, opts: pulumi.ResourceOptions
    ) -> List[pulumi.Resource]:
        gcs_account = pulumi_gcp.storage.get_project_service_account(
            project=self.gcs_bucket.project_id,
            user_project=self.gcs_bucket.project_id,
        )
        binding = pulumi_gcp.pubsub.TopicIAMBinding(
            f"{self.primitive_id()}_binding",
            opts=opts,
            topic=self.pubsub_subscription.topic.topic_name,
            role="roles/pubsub.publisher",
            project=self.pubsub_subscription.topic.project_id,
            members=[f"serviceAccount:{gcs_account.email_address}"],
        )

        opts = pulumi.ResourceOptions.merge(
            opts, pulumi.ResourceOptions(depends_on=[binding])
        )

        notification = pulumi_gcp.storage.Notification(
            f"{self.primitive_id()}_notification",
            opts=opts,
            bucket=self.gcs_bucket.bucket_name,
            topic=self.pubsub_subscription.topic.topic_id,
            payload_format="JSON_API_V1",
            event_types=[et.name for et in self.event_types],
        )
        return [binding, notification]
