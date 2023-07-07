import pulumi_gcp

from buildflow.core.io.providers._provider import PulumiProvider, SinkProvider
from buildflow.core.io.providers.gcp.pubsub.topic.strategies.sink import (
    GCPPubSubTopicSink,
)
from buildflow.core.resources.pulumi import PulumiResource


class GCPPubSubTopicProvider(SinkProvider, PulumiProvider):
    def __init__(self, project_id: str, topic_name: str):
        self.project_id = project_id
        self.topic_name = topic_name

    def sink(self):
        return GCPPubSubTopicSink(
            project_id=self.project_id,
            topic_name=self.topic_name,
        )

    def pulumi_resources(self):
        topic_resource = pulumi_gcp.pubsub.Topic(
            self.topic_name, name=self.topic_name, project=self.project_id
        )
        return [
            PulumiResource(
                resource_id=self.topic_name,
                resource=topic_resource,
                exports={"gcp.pubsub.topic.name": topic_resource.name},
            )
        ]
