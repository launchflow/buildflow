from typing import Iterable, List, Optional, Type

import pulumi
import pulumi_gcp

from buildflow.core.credentials import GCPCredentials
from buildflow.core.io.gcp.providers.pubsub_providers import (
    GCPPubSubSubscriptionProvider, GCPPubSubTopicProvider)
from buildflow.core.io.gcp.providers.storage_providers import GCSBucketProvider
from buildflow.core.io.gcp.strategies.composite_strategies import \
    GCSFileChangeStreamSource
from buildflow.core.providers.provider import PulumiProvider, SourceProvider
from buildflow.core.resources.pulumi import PulumiResource
from buildflow.core.types.gcp_types import (GCPProjectID,
                                            GCSChangeStreamEventType)


class GCSFileChangeStreamProvider(SourceProvider, PulumiProvider):
    def __init__(
        self,
        *,
        gcs_bucket_provider: Optional[GCSBucketProvider],
        # NOTE: pubsub_topic_provider is only needed as a PulumiProvider, so its
        # optional for the case where we only want to use the source_provider
        pubsub_topic_provider: Optional[GCPPubSubTopicProvider],
        pubsub_subscription_provider: Optional[GCPPubSubSubscriptionProvider],
        project_id: GCPProjectID,
        # source-only options
        event_types: Iterable[GCSChangeStreamEventType],
        # pulumi-only options
        destroy_protection: bool = False,
        bucket_managed: bool = False,
        subscription_managed: bool = False,
        topic_managed: bool = False,
    ):
        self.gcs_bucket_provider = gcs_bucket_provider
        self.pubsub_topic_provider = pubsub_topic_provider
        self.pubsub_subscription_provider = pubsub_subscription_provider
        self.project_id = project_id
        # source-only options
        self.event_types = list(event_types)
        # pulumi-only options
        self.destroy_protection = destroy_protection
        self.bucket_managed = bucket_managed
        self.subscription_managed = subscription_managed
        self.topic_managed = topic_managed

    def source(self, credentials: GCPCredentials):
        return GCSFileChangeStreamSource(
            project_id=self.project_id,
            credentials=credentials,
            pubsub_source=self.pubsub_subscription_provider.source(credentials),
        )

    def pulumi_resources(
        self, type_: Optional[Type], depends_on: List[PulumiResource] = []
    ) -> List[PulumiResource]:
        if self.gcs_bucket_provider is None:
            raise ValueError(
                "Cannot create Pulumi resources for GCSFileStreamProvider without a "
                "GCSBucketProvider."
            )
        gcs_resources = []
        topic_resources = []
        subscription_resources = []
        # Set up GCP bucket
        if self.bucket_managed:
            gcs_resources = self.gcs_bucket_provider.pulumi_resources(type_)
        gcs_pulumi_resources = [tr.resource for tr in gcs_resources]
        # Set up pubsub topic
        if self.topic_managed:
            topic_resources = self.pubsub_topic_provider.pulumi_resources(type_)
        gcs_account = pulumi_gcp.storage.get_project_service_account(
            project=self.gcs_bucket_provider.project_id,
            user_project=self.gcs_bucket_provider.project_id,
        )
        topic_pulumi_resources = [tr.resource for tr in topic_resources]
        binding = pulumi_gcp.pubsub.TopicIAMBinding(
            "binding",
            opts=pulumi.ResourceOptions(depends_on=topic_pulumi_resources),
            topic=self.pubsub_topic_provider.topic_name,
            role="roles/pubsub.publisher",
            project=self.pubsub_topic_provider.project_id,
            members=[f"serviceAccount:{gcs_account.email_address}"],
        )

        # Set up GCS nofitifacion
        notification_depends_on = (
            gcs_pulumi_resources + topic_pulumi_resources + [binding]
        )
        notification = pulumi_gcp.storage.Notification(
            f"{self.gcs_bucket_provider.bucket_name}_notification",
            opts=pulumi.ResourceOptions(depends_on=notification_depends_on),
            bucket=self.gcs_bucket_provider.bucket_name,
            topic=self.pubsub_topic_provider.topic_id,
            payload_format="JSON_API_V1",
            event_types=[et.name for et in self.event_types],
        )
        notification_resource = PulumiResource(
            resource_id=notification.id, resource=notification, exports={}
        )
        binding_resource = PulumiResource(
            resource_id=binding.id, resource=binding, exports={}
        )

        # Setup pubsub subscription
        if self.subscription_managed:
            subscription_resources = self.pubsub_subscription_provider.pulumi_resources(
                type_, depends_on=topic_resources
            )
        return (
            gcs_resources
            + topic_resources
            + subscription_resources
            + [notification_resource, binding_resource]
        )
