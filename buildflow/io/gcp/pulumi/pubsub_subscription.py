import pulumi
import pulumi_gcp

from buildflow.core.credentials import GCPCredentials
from buildflow.core.types.gcp_types import GCPProjectID, PubSubSubscriptionName
from buildflow.io.gcp.pubsub_topic import GCPPubSubTopic


class PubSubSubscriptionPulumiResource(pulumi.ComponentResource):
    def __init__(
        self,
        # subscription primitive options
        subscription_name: PubSubSubscriptionName,
        project_id: GCPProjectID,
        topic: GCPPubSubTopic,
        ack_deadline_seconds: int,
        message_retention_duration: str,
        # pulumi_resource options (buildflow internal concept)
        credentials: GCPCredentials,
        opts: pulumi.ResourceOptions,
    ):
        super().__init__(
            "buildflow:gcp:pubsub:Subscription",
            f"buildflow-{project_id}-{subscription_name}",
            None,
            opts,
        )

        self.subscription_resource = pulumi_gcp.pubsub.Subscription(
            resource_name=f"{project_id}-{subscription_name}",
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
                "gcp.pubsub.subscription.topic": self.subscription_resource.topic,
                "buildflow.cloud_console.url": f"https://console.cloud.google.com/cloudpubsub/subscription/detail/{subscription_name}?project={project_id}",
            }
        )
