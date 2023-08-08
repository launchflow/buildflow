import dataclasses
from typing import Optional

from buildflow.config.cloud_provider_config import GCPOptions
from buildflow.core import utils
from buildflow.core.options.runtime_options import RuntimeOptions
from buildflow.core.types.gcp_types import GCPProjectID, PubSubSubscriptionName
from buildflow.core.types.portable_types import SubscriptionName
from buildflow.io.gcp.providers.pubsub_subscription import GCPPubSubSubscriptionProvider
from buildflow.io.gcp.pubsub_topic import GCPPubSubTopic
from buildflow.io.primitive import GCPPrimtive

_DEFAULT_ACK_DEADLINE_SECONDS = 10 * 60
_DEFAULT_MESSAGE_RETENTION_DURATION = "1200s"


# NOTE: A user should use this in the case where they want to connect to an existing
# topic.
@dataclasses.dataclass
class GCPPubSubSubscription(
    GCPPrimtive[
        # Pulumi provider type
        GCPPubSubSubscriptionProvider,
        # Source provider type
        GCPPubSubSubscriptionProvider,
        # Sink provider type
        None,
        # Background task provider type
        None,
    ]
):
    project_id: GCPProjectID
    subscription_name: PubSubSubscriptionName
    # required fields
    topic: GCPPubSubTopic
    # Optional fields
    batch_size: int = 1000
    include_attributes: bool = False
    # pulumi options
    ack_deadline_seconds: bool = dataclasses.field(
        default=_DEFAULT_ACK_DEADLINE_SECONDS, init=False
    )
    message_retention_duration: str = dataclasses.field(
        default=_DEFAULT_MESSAGE_RETENTION_DURATION, init=False
    )
    topic: Optional[GCPPubSubTopic] = dataclasses.field(default=None, init=False)

    def options(
        self,
        managed: bool = False,
        ack_deadline_seconds: bool = _DEFAULT_ACK_DEADLINE_SECONDS,
        message_retention_duration: str = _DEFAULT_MESSAGE_RETENTION_DURATION,
        topic: Optional[GCPPubSubTopic] = None,
    ) -> "GCPPubSubSubscription":
        to_ret = super().options(managed)
        to_ret.ack_deadline_seconds = ack_deadline_seconds
        to_ret.message_retention_duration = message_retention_duration
        to_ret.topic = topic
        return to_ret

    @classmethod
    def from_gcp_options(
        cls,
        gcp_options: GCPOptions,
        *,
        topic: GCPPubSubTopic,
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
            topic=topic,
        )

    def source_provider(self) -> GCPPubSubSubscriptionProvider:
        # TODO: Add support to supply the source-only options. Maybe add some kind of
        # "inject_options" method for the different provider types.
        # Use a Builder pattern for this.
        return GCPPubSubSubscriptionProvider(
            project_id=self.project_id,
            subscription_name=self.subscription_name,
            topic=self.topic,
            batch_size=self.batch_size,
            include_attributes=self.include_attributes,
            ack_deadline_seconds=self.ack_deadline_seconds,
            message_retention_duration=self.message_retention_duration,
        )

    # NOTE: Subscriptions do not support sinks, but we "implement" it here to
    # give the user a better error message since this is a common mistake.
    def sink_provider(self, runtime_options: RuntimeOptions):
        raise ValueError(
            "GCPPubSubSubscription does not support sink_provider()."
            "Please use GCPPubSubTopic instead."
        )

    def _pulumi_provider(self) -> GCPPubSubSubscriptionProvider:
        return GCPPubSubSubscriptionProvider(
            project_id=self.project_id,
            subscription_name=self.subscription_name,
            topic=self.topic,
            batch_size=self.batch_size,
            include_attributes=self.include_attributes,
            ack_deadline_seconds=self.ack_deadline_seconds,
            message_retention_duration=self.message_retention_duration,
        )
