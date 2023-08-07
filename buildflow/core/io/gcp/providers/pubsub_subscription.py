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


class GCPPubSubSubscriptionProvider(SourceProvider, PulumiProvider):
    def __init__(
        self,
        *,
        project_id: GCPProjectID,
        subscription_name: PubSubSubscriptionName,
        topic: GCPPubSubTopic,
        # source-only options
        batch_size: int = 1000,
        include_attributes: bool = False,
        # pulumi-only options
        ack_deadline_seconds: int = 10 * 60,
        message_retention_duration: str = "1200s",
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

    def pulumi(
        self,
        type_: Optional[Type],
        credentials: GCPCredentials,
    ):
        class Subscription(pulumi.ComponentResource):
            def __init__(
                self,
                subscription_name: PubSubSubscriptionName,
                project_id: GCPProjectID,
                topic: GCPPubSubTopic,
                ack_deadline_seconds: int,
                message_retention_duration: str,
            ):
                name = f"buildflow-{subscription_name}"
                props: pulumi.Inputs | None = None
                opts: pulumi.ResourceOptions | None = None
                if topic.managed:
                    topic_resource = topic.pulumi_provider().pulumi(type_, credentials)
                    opts = pulumi.ResourceOptions(depends_on=topic_resource)
                super().__init__("buildflow:gcp:pubsub:Subscription", name, props, opts)

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

        return Subscription(
            self.subscription_name,
            self.project_id,
            self.topic,
            self.ack_deadline_seconds,
            self.message_retention_duration,
        )
