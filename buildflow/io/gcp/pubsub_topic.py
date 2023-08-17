import dataclasses
from typing import Optional

from buildflow.config.cloud_provider_config import GCPOptions
from buildflow.core import utils
from buildflow.core.types.gcp_types import GCPProjectID, PubSubTopicID, PubSubTopicName
from buildflow.core.types.portable_types import TopicName
from buildflow.io.gcp.providers.pubsub_topic import GCPPubSubTopicProvider
from buildflow.io.primitive import GCPPrimtive


@dataclasses.dataclass
class GCPPubSubTopic(
    GCPPrimtive[
        # Pulumi provider type
        GCPPubSubTopicProvider,
        # Source provider type
        None,
        # Sink provider type
        GCPPubSubTopicProvider,
        # Background task provider type
        None,
    ]
):
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

    def sink_provider(self) -> GCPPubSubTopicProvider:
        return GCPPubSubTopicProvider(
            project_id=self.project_id,
            topic_name=self.topic_name,
        )

    def _pulumi_provider(self) -> GCPPubSubTopicProvider:
        return GCPPubSubTopicProvider(
            project_id=self.project_id,
            topic_name=self.topic_name,
        )
