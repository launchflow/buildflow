import pulumi
import pulumi_gcp

from buildflow.core.credentials import GCPCredentials
from buildflow.core.types.gcp_types import GCPProjectID, PubSubTopicName


class GCPPubSubTopicResource(pulumi.ComponentResource):
    def __init__(
        self,
        project_id: GCPProjectID,
        topic_name: PubSubTopicName,
        # pulumi_resource options (buildflow internal concept)
        credentials: GCPCredentials,
        opts: pulumi.ResourceOptions,
    ):
        super().__init__(
            "buildflow:gcp:storage:PubSubTopic",
            f"buildflow-{project_id}-{topic_name}",
            None,
            opts,
        )

        outputs = {}

        self.topic_resource = pulumi_gcp.pubsub.Topic(
            resource_name=f"{project_id}-{topic_name}",
            opts=pulumi.ResourceOptions(parent=self),
            name=topic_name,
            project=project_id,
        )

        outputs["gcp.pubsub.topic"] = self.topic_resource.id
        outputs[
            "buildflow.cloud_console.url"
        ] = f"https://console.cloud.google.com/cloudpubsub/topic/detail/{topic_name}?project={project_id}"

        self.register_outputs(outputs)
