import dataclasses
from typing import List, Optional

import pulumi
import pulumi_gcp

from buildflow.config.cloud_provider_config import GCPOptions
from buildflow.core import utils
from buildflow.core.credentials.gcp_credentials import GCPCredentials
from buildflow.core.types.gcp_types import GCPProjectID, PubSubSubscriptionName
from buildflow.core.types.portable_types import SubscriptionName
from buildflow.io.gcp.pubsub_topic import GCPPubSubTopic
from buildflow.io.gcp.strategies.pubsub_strategies import GCPPubSubSubscriptionSource
from buildflow.io.primitive import GCPPrimtive

_DEFAULT_ACK_DEADLINE_SECONDS = 10 * 60
_DEFAULT_MESSAGE_RETENTION_DURATION = "1200s"
_DEFAULT_BATCH_SIZE = 1_000
_DEFAULT_INCLUDE_ATTRIBUTES = False
_DEFAULT_ENABLE_EXACTLY_ONCE_DELIVERY = False


# NOTE: A user should use this in the case where they want to connect to an existing
# topic.
@dataclasses.dataclass
class GCPPubSubSubscription(GCPPrimtive):
    project_id: GCPProjectID
    subscription_name: PubSubSubscriptionName
    # required fields
    batch_size: int = dataclasses.field(default=_DEFAULT_BATCH_SIZE, init=False)
    include_attributes: bool = dataclasses.field(
        default=_DEFAULT_INCLUDE_ATTRIBUTES, init=False
    )
    # pulumi options
    ack_deadline_seconds: bool = dataclasses.field(
        default=_DEFAULT_ACK_DEADLINE_SECONDS, init=False
    )
    message_retention_duration: str = dataclasses.field(
        default=_DEFAULT_MESSAGE_RETENTION_DURATION, init=False
    )
    topic: Optional[GCPPubSubTopic] = dataclasses.field(default=None, init=False)
    enable_exactly_once_delivery: bool = dataclasses.field(
        default=_DEFAULT_ENABLE_EXACTLY_ONCE_DELIVERY, init=False
    )

    def options(
        self,
        # Pulumi management options
        ack_deadline_seconds: bool = _DEFAULT_ACK_DEADLINE_SECONDS,
        message_retention_duration: str = _DEFAULT_MESSAGE_RETENTION_DURATION,
        enable_exactly_once_delivery: bool = _DEFAULT_ENABLE_EXACTLY_ONCE_DELIVERY,
        topic: Optional[GCPPubSubTopic] = None,
        # Source options
        batch_size: int = _DEFAULT_BATCH_SIZE,
        include_attributes: bool = _DEFAULT_INCLUDE_ATTRIBUTES,
    ) -> "GCPPubSubSubscription":
        self.ack_deadline_seconds = ack_deadline_seconds
        self.message_retention_duration = message_retention_duration
        self.topic = topic
        self.batch_size = batch_size
        self.include_attributes = include_attributes
        self.enable_exactly_once_delivery = enable_exactly_once_delivery
        return self

    def primitive_id(self):
        return f"{self.project_id}/{self.subscription_name}"

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

    def source(self, credentials: GCPCredentials) -> GCPPubSubSubscriptionSource:
        return GCPPubSubSubscriptionSource(
            credentials=credentials,
            project_id=self.project_id,
            subscription_name=self.subscription_name,
            batch_size=self.batch_size,
            include_attributes=self.include_attributes,
        )

    def pulumi_resources(
        self, credentials: GCPCredentials, opts: pulumi.ResourceOptions
    ) -> List[pulumi.Resource]:
        if self.topic is None:
            raise ValueError(
                "A topic must be provided to the GCPPubSubSubscription. Please provide "
                "one via GCPPubSubSubscription(...).options(topic=GCPPubSubTopic(...))"
                " method."
            )
        return [
            pulumi_gcp.pubsub.Subscription(
                resource_name=f"{self.project_id}-{self.subscription_name}",
                opts=opts,
                name=self.subscription_name,
                topic=self.topic.topic_id,
                project=self.project_id,
                ack_deadline_seconds=self.ack_deadline_seconds,
                message_retention_duration=self.message_retention_duration,
                enable_exactly_once_delivery=self.enable_exactly_once_delivery,
            )
        ]

    def cloud_console_url(self) -> str:
        return f"https://console.cloud.google.com/cloudpubsub/subscription/detail/{self.subscription_name}?project={self.project_id}"
