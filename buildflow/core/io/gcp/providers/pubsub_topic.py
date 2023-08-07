from typing import Optional, Type

import pulumi
import pulumi_gcp

from buildflow.core.credentials import GCPCredentials
from buildflow.core.io.gcp.strategies.pubsub_strategies import GCPPubSubTopicSink
from buildflow.core.providers.provider import PulumiProvider, SinkProvider
from buildflow.core.types.gcp_types import GCPProjectID, PubSubTopicID, PubSubTopicName


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
    ):
        # TODO: Support schemas for topics
        del type_

        class Topic(pulumi.ComponentResource):
            def __init__(self, topic_name: PubSubTopicName, project_id: GCPProjectID):
                name = f"buildflow-{topic_name}"
                props: pulumi.Inputs | None = (None,)
                opts: pulumi.ResourceOptions | None = None
                super().__init__("buildflow:gcp:pubsub:Topic", name, props, opts)

                self.topic_resource = pulumi_gcp.pubsub.Topic(
                    opts=pulumi.ResourceOptions(parent=self),
                    name=topic_name,
                    project=project_id,
                )
                self.register_outputs(
                    {
                        "gcp.pubsub.topic.name": self.topic_resource.name,
                    }
                )

        return Topic(self.topic_name, self.project_id)
