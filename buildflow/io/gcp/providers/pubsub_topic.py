from typing import Optional, Type

import pulumi
import pulumi_gcp

from buildflow.core.credentials import GCPCredentials
from buildflow.core.types.gcp_types import GCPProjectID, PubSubTopicID, PubSubTopicName
from buildflow.io.gcp.strategies.pubsub_strategies import GCPPubSubTopicSink
from buildflow.io.provider import PulumiProvider, SinkProvider


class _PubSubTopic(pulumi.ComponentResource):
    def __init__(
        self,
        project_id: GCPProjectID,
        topic_name: PubSubTopicName,
        # pulumi_resource options (buildflow internal concept)
        type_: Optional[Type],
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

    def pulumi_resource(
        self,
        type_: Optional[Type],
        credentials: GCPCredentials,
        opts: pulumi.ResourceOptions,
    ):
        # TODO: Support schemas for topics
        return _PubSubTopic(
            topic_name=self.topic_name,
            project_id=self.project_id,
            type_=type_,
            credentials=credentials,
            opts=opts,
        )
