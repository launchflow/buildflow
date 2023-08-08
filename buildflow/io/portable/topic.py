import dataclasses

from buildflow.config.cloud_provider_config import CloudProvider, CloudProviderConfig
from buildflow.core.types.portable_types import TopicID
from buildflow.io.gcp.pubsub_subscription import GCPPubSubSubscription
from buildflow.io.gcp.pubsub_topic import GCPPubSubTopic
from buildflow.io.primitive import PortablePrimtive, Primitive
from buildflow.io.strategies._strategy import StategyType


@dataclasses.dataclass
class Topic(PortablePrimtive):
    topic_id: TopicID

    def to_cloud_primitive(
        self, cloud_provider_config: CloudProviderConfig, strategy_type: StategyType
    ) -> Primitive:
        # GCP Implementations
        if cloud_provider_config.default_cloud_provider == CloudProvider.GCP:
            if strategy_type == StategyType.SOURCE:
                return GCPPubSubSubscription.from_gcp_options(
                    gcp_options=cloud_provider_config.gcp_options,
                    topic_id=self.topic_id,
                )
            elif strategy_type == StategyType.SINK:
                return GCPPubSubTopic.from_gcp_options(
                    gcp_options=cloud_provider_config.gcp_options
                )
            else:
                raise ValueError(
                    f"Unsupported strategy type for Topic (GCP): {strategy_type}"
                )
        # AWS Implementations
        elif cloud_provider_config.default_cloud_provider == CloudProvider.AWS:
            raise NotImplementedError("AWS is not implemented for Topic.")
        # Azure Implementations
        elif cloud_provider_config.default_cloud_provider == CloudProvider.AZURE:
            raise NotImplementedError("Azure is not implemented for Topic.")
        # Local Implementations
        elif cloud_provider_config.default_cloud_provider == CloudProvider.LOCAL:
            raise NotImplementedError("Local is not implemented for Topic.")
        # Sanity check
        else:
            raise ValueError(
                f"Unknown resource provider: {cloud_provider_config.default_cloud_provider}"  # noqa: E501
            )
