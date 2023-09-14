import dataclasses
from typing import Optional

import pulumi

from buildflow.config.cloud_provider_config import GCPOptions
from buildflow.core import utils
from buildflow.core.credentials.gcp_credentials import GCPCredentials
from buildflow.core.types.gcp_types import GCPProjectID, PubSubTopicID, PubSubTopicName
from buildflow.core.types.portable_types import TopicName
from buildflow.io.gcp.pulumi.pubsub_topic import GCPPubSubTopicResource
from buildflow.io.gcp.strategies.pubsub_strategies import GCPPubSubTopicSink
from buildflow.io.primitive import GCPPrimtive
from buildflow.io.strategies.sink import SinkStrategy


@dataclasses.dataclass
class GCPPubSubTopic(GCPPrimtive):
    project_id: GCPProjectID
    topic_name: PubSubTopicName

    @property
    def topic_id(self) -> PubSubTopicID:
        return f"projects/{self.project_id}/topics/{self.topic_name}"

    @classmethod
    def from_gcp_options(
        cls, gcp_options: GCPOptions, topic_name: Optional[TopicName] = None
    ) -> "GCPPubSubTopic":
        project_id = gcp_options.default_project_id
        project_hash = utils.stable_hash(project_id)
        if topic_name is None:
            topic_name = f"buildflow_topic_{project_hash[:8]}"
        return cls(
            project_id=project_id,
            topic_name=topic_name,
        )

    def sink(self, credentials: GCPCredentials) -> SinkStrategy:
        return GCPPubSubTopicSink(
            credentials=credentials,
            project_id=self.project_id,
            topic_name=self.topic_name,
        )

    def pulumi_resource(
        self, credentials: GCPCredentials, opts: pulumi.ResourceOptions
    ) -> GCPPubSubTopicResource:
        return GCPPubSubTopicResource(
            credentials=credentials,
            project_id=self.project_id,
            topic_name=self.topic_name,
            opts=opts,
        )
