import pulumi_gcp

from buildflow.core.io.providers._provider import PulumiProvider, SourceProvider
from buildflow.core.io.providers.gcp.pubsub.subscription.strategies.source import (
    GCPPubSubSubscriptionSource,
)
from buildflow.core.resources.pulumi import PulumiResource


class GCPPubSubSubscriptionProvider(SourceProvider, PulumiProvider):
    def __init__(
        self,
        *,
        subscription_name: str,
        topic_id: str,
        project_id: str,
        # source-only options
        batch_size: str = 1000,
        include_attributes: bool = False,
        # infra-only options
        ack_deadline_seconds: int = 10 * 60,
        message_retention_duration: str = "1200s",
    ):
        self.subscription_name = subscription_name
        self.topic_id = topic_id
        self.project_id = project_id
        self.batch_size = batch_size
        self.include_attributes = include_attributes
        self.ack_deadline_seconds = ack_deadline_seconds
        self.message_retention_duration = message_retention_duration

    def source(self):
        return GCPPubSubSubscriptionSource(
            subscription_name=self.subscription_name,
            topic_id=self.topic_id,
            project_id=self.project_id,
            batch_size=self.batch_size,
            include_attributes=self.include_attributes,
        )

    def pulumi_resources(self):
        subscription_resource = pulumi_gcp.pubsub.Subscription(
            # NOTE: resource_name is the name of the resource in Pulumi state, not gcp
            self.subscription_name,
            name=self.subscription_name,
            topic=self.topic_id,
            project=self.project_id,
            ack_deadline_seconds=self.ack_deadline_seconds,
            message_retention_duration=self.message_retention_duration,
        )
        return [
            PulumiResource(
                resource_id="",
                resource=subscription_resource,
                exports={},
            )
        ]
