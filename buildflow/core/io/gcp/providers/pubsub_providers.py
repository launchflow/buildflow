from typing import List, Optional, Type

import pulumi
import pulumi_gcp

from buildflow.core.credentials import GCPCredentials
from buildflow.core.io.gcp.strategies.pubsub_strategies import (
    GCPPubSubSubscriptionSource,
    GCPPubSubTopicSink,
)
from buildflow.core.providers.provider import (
    PulumiProvider,
    SinkProvider,
    SourceProvider,
)
from buildflow.core.resources.pulumi import PulumiResource
from buildflow.core.types.gcp_types import (
    GCPProjectID,
    PubSubSubscriptionName,
    PubSubTopicID,
    PubSubTopicName,
)


class GCPPubSubTopicProvider(SinkProvider, PulumiProvider):
    def __init__(
        self,
        *,
        project_id: GCPProjectID,
        topic_name: PubSubTopicName,
    ):
        self.project_id = project_id
        self.topic_name = topic_name

    @property
    def topic_id(self) -> PubSubTopicID:
        return f"projects/{self.project_id}/topics/{self.topic_name}"

    def sink(self, credentials: GCPCredentials):
        return GCPPubSubTopicSink(
            credentials=credentials,
            project_id=self.project_id,
            topic_name=self.topic_name,
        )

    def pulumi_resources(
        self, type_: Optional[Type], depends_on: List[PulumiResource] = []
    ):
        # TODO: Support schemas for topics
        del type_
        topic_resource = pulumi_gcp.pubsub.Topic(
            self.topic_name, name=self.topic_name, project=self.project_id
        )
        pulumi.export("gcp.pubsub.topic.name", topic_resource.name)
        return [
            PulumiResource(
                resource_id=self.topic_name,
                resource=topic_resource,
                exports={"gcp.pubsub.topic.name": topic_resource.name},
            )
        ]


class GCPPubSubSubscriptionProvider(SourceProvider, PulumiProvider):
    def __init__(
        self,
        *,
        project_id: GCPProjectID,
        subscription_name: PubSubSubscriptionName,
        topic_id: PubSubTopicID,
        # source-only options
        batch_size: int = 1000,
        include_attributes: bool = False,
        # pulumi-only options
        ack_deadline_seconds: int = 10 * 60,
        message_retention_duration: str = "1200s",
    ):
        self.project_id = project_id
        self.subscription_name = subscription_name
        self.topic_id = topic_id
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

    def pulumi_resources(
        self, type_: Optional[Type], depends_on: List[PulumiResource] = []
    ):
        depends = [tr.resource for tr in depends_on]
        subscription_resource = pulumi_gcp.pubsub.Subscription(
            self.subscription_name,
            name=self.subscription_name,
            topic=self.topic_id,
            opts=pulumi.ResourceOptions(depends_on=depends),
            project=self.project_id,
            ack_deadline_seconds=self.ack_deadline_seconds,
            message_retention_duration=self.message_retention_duration,
        )
        pulumi.export("gcp.pubsub.subscription.name", subscription_resource.name)
        pulumi.export("gcp.pubsub.subscription.topic", subscription_resource.topic)
        return [
            PulumiResource(
                resource_id=self.subscription_name,
                resource=subscription_resource,
                exports={
                    "gcp.pubsub.subscription.name": subscription_resource.name,
                    "gcp.pubsub.subscription.topic": subscription_resource.topic,
                },
            )
        ]
