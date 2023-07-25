import dataclasses
from typing import Optional

from buildflow.core import utils
from buildflow.core.options.runtime_options import RuntimeOptions
from buildflow.core.io.gcp.providers.pubsub_providers import (
    GCPPubSubTopicProvider,
    GCPPubSubSubscriptionProvider,
)
from buildflow.core.io.primitive import GCPPrimtive
from buildflow.config.cloud_provider_config import GCPOptions
from buildflow.core.types.gcp_types import (
    GCPProjectID,
    PubSubSubscriptionName,
    PubSubTopicID,
    PubSubTopicName,
)
from buildflow.core.types.portable_types import TopicID, TopicName, SubscriptionName


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

    # NOTE: Topics do not support sinks, but we "implement" it here to
    # give the user a better error message since this is a common mistake.
    def source_provider(self):
        raise ValueError(
            "GCPPubSubTopic does not support source_provider()."
            "Please use GCPPubSubSubscription instead."
        )

    def sink_provider(self) -> GCPPubSubTopicProvider:
        return GCPPubSubTopicProvider(
            project_id=self.project_id,
            topic_name=self.topic_name,
        )

    def pulumi_provider(self) -> GCPPubSubTopicProvider:
        return GCPPubSubTopicProvider(
            project_id=self.project_id,
            topic_name=self.topic_name,
        )


# NOTE: A user should use this in the case where they want to connect to an existing
# topic.
@dataclasses.dataclass
class GCPPubSubSubscription(GCPPrimtive):
    project_id: GCPProjectID
    subscription_name: PubSubSubscriptionName
    # required fields
    topic_id: PubSubTopicID

    # Optional fields
    include_attributes: bool = False

    @classmethod
    def from_gcp_options(
        cls,
        gcp_options: GCPOptions,
        *,
        topic_id: TopicID,
        subscription_name: Optional[SubscriptionName] = None,
    ) -> "GCPPubSubSubscription":
        project_id = gcp_options.default_project_id
        if project_id is None:
            raise ValueError(
                "No Project ID was provided in the GCP options. Please provide one in "
                "the .buildflow config."
            )
        project_hash = utils.stable_hash(project_id)
        if subscription_name is None:
            subscription_name = f"buildflow_subscription_{project_hash[:8]}"
        return cls(
            project_id=project_id,
            subscription_name=subscription_name,
            topic_id=topic_id,
        )

    def source_provider(self) -> GCPPubSubSubscriptionProvider:
        # TODO: Add support to supply the source-only options. Maybe add some kind of
        # "inject_options" method for the different provider types.
        # Use a Builder pattern for this.
        return GCPPubSubSubscriptionProvider(
            project_id=self.project_id,
            subscription_name=self.subscription_name,
            topic_id=self.topic_id,
            include_attributes=self.include_attributes,
        )

    # NOTE: Subscriptions do not support sinks, but we "implement" it here to
    # give the user a better error message since this is a common mistake.
    def sink_provider(self, runtime_options: RuntimeOptions):
        raise ValueError(
            "GCPPubSubSubscription does not support sink_provider()."
            "Please use GCPPubSubTopic instead."
        )

    def pulumi_provider(self) -> GCPPubSubSubscriptionProvider:
        # TODO: Add support to supply the pulumi-only options
        return GCPPubSubSubscriptionProvider(
            project_id=self.project_id,
            subscription_name=self.subscription_name,
            topic_id=self.topic_id,
        )
