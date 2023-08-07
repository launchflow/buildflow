from typing import Optional, Type

import pulumi
import pulumi_gcp

from buildflow.core.credentials import GCPCredentials
from buildflow.core.io.gcp.strategies.pubsub_strategies import (
    GCPPubSubSubscriptionSource,
)
from buildflow.core.providers.provider import PulumiProvider, SourceProvider
from buildflow.core.types.gcp_types import GCPProjectID, PubSubSubscriptionName
from buildflow.io.gcp.pubsub_topic import GCPPubSubTopic


class _PubSubSubscriptionPulumiResource(pulumi.ComponentResource):
    def __init__(
        self,
        # subscription primitive options
        subscription_name: PubSubSubscriptionName,
        project_id: GCPProjectID,
        topic: GCPPubSubTopic,
        ack_deadline_seconds: int,
        message_retention_duration: str,
        # pulumi_resource options (buildflow internal concept)
        type_: Optional[Type],
        credentials: GCPCredentials,
        opts: pulumi.ResourceOptions,
    ):
        topic_resource = topic.pulumi_provider().pulumi_resource(
            type_, credentials, opts
        )
        if topic_resource is not None:
            opts = pulumi.ResourceOptions.merge(
                opts, pulumi.ResourceOptions(depends_on=topic_resource)
            )
        super().__init__(
            "buildflow:gcp:pubsub:Subscription",
            f"buildflow-{project_id}-{subscription_name}",
            None,
            opts,
        )

        self.subscription_resource = pulumi_gcp.pubsub.Subscription(
            opts=pulumi.ResourceOptions(parent=self),
            name=subscription_name,
            topic=topic.topic_id,
            project=project_id,
            ack_deadline_seconds=ack_deadline_seconds,
            message_retention_duration=message_retention_duration,
        )
        self.register_outputs(
            {
                "gcp.pubsub.subscription.name": self.subscription_resource.name,
                "gcp.pubsub.subscription.topic": self.subscription_resource.topic,  # noqa: E501
            }
        )


class GCPPubSubSubscriptionProvider(SourceProvider, PulumiProvider):
    def __init__(
        self,
        *,
        project_id: GCPProjectID,
        subscription_name: PubSubSubscriptionName,
        topic: GCPPubSubTopic,
        # source-only options
        batch_size: int,
        include_attributes: bool,
        # pulumi-only options
        ack_deadline_seconds: int,
        message_retention_duration: str,
    ):
        self.project_id = project_id
        self.subscription_name = subscription_name
        self.topic = topic
        # source-only options
        self.batch_size = batch_size
        self.include_attributes = include_attributes
        # pulumi-only options
        self.ack_deadline_seconds = ack_deadline_seconds
        self.message_retention_duration = message_retention_duration

    def source(self, credentials: GCPCredentials):
        return GCPPubSubSubscriptionSource(
            credentials=credentials,
            subscription_name=self.subscription_name,
            project_id=self.project_id,
            batch_size=self.batch_size,
            include_attributes=self.include_attributes,
        )

    def pulumi_resource(
        self,
        type_: Optional[Type],
        credentials: GCPCredentials,
        opts: pulumi.ResourceOptions,
    ):
        return _PubSubSubscriptionPulumiResource(
            self.subscription_name,
            self.project_id,
            self.topic,
            self.ack_deadline_seconds,
            self.message_retention_duration,
            type_,
            credentials,
            opts,
        )
