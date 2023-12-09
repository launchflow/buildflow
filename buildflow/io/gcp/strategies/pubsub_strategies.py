import dataclasses
import datetime
import logging
from typing import Any, Callable, Iterable, Optional, Type, Union

from google.cloud.monitoring_v3 import query
from google.cloud.pubsub_v1.types import PubsubMessage as GCPPubSubMessage
from google.protobuf.timestamp_pb2 import Timestamp

from buildflow import exceptions
from buildflow.core import utils
from buildflow.core.credentials import GCPCredentials
from buildflow.core.types.gcp_types import (
    GCPProjectID,
    PubSubSubscriptionID,
    PubSubSubscriptionName,
    PubSubTopicID,
    PubSubTopicName,
)
from buildflow.io.strategies.sink import Batch, SinkStrategy
from buildflow.io.strategies.source import AckInfo, PullResponse, SourceStrategy
from buildflow.io.utils.clients import gcp_clients
from buildflow.io.utils.schemas import converters
from buildflow.types.gcp import PubsubMessage


@dataclasses.dataclass(frozen=True)
class _PubsubAckInfo(AckInfo):
    ack_ids: Iterable[str]


def _timestamp_to_datetime(timestamp: Union[datetime.datetime, Timestamp]):
    if isinstance(timestamp, Timestamp):
        return timestamp.ToDatetime()
    return timestamp


class GCPPubSubSubscriptionSource(SourceStrategy):
    def __init__(
        self,
        *,
        credentials: GCPCredentials,
        subscription_name: PubSubSubscriptionName,
        project_id: GCPProjectID,
        batch_size: int = 1000,
        include_attributes: bool = False,
    ):
        super().__init__(
            credentials=credentials,
            strategy_id="gcp-pubsub-subscription-source",
        )
        # configuration
        self.subscription_name = subscription_name
        self.project_id = project_id
        self.batch_size = batch_size
        self.include_attributes = include_attributes
        # setup
        self.credentials = credentials
        clients = gcp_clients.GCPClients(
            credentials=credentials,
            quota_project_id=project_id,
        )
        self.subscriber_client = clients.get_async_subscriber_client()
        self.publisher_client = clients.get_async_publisher_client()
        self.metrics_client = clients.get_metrics_client()
        # initial state

    @property
    def subscription_id(self) -> PubSubSubscriptionID:
        return f"projects/{self.project_id}/subscriptions/{self.subscription_name}"  # noqa: E501

    async def pull(self) -> PullResponse:
        try:
            response = await self.subscriber_client.pull(
                subscription=self.subscription_id,
                max_messages=self.batch_size,
            )
        except Exception as e:
            logging.error("pubsub pull failed with: %s", e)
            return PullResponse([], _PubsubAckInfo([]))

        payloads = []
        ack_ids = []
        for received_message in response.received_messages:
            if self.include_attributes:
                att_dict = {}
                attributes = received_message.message.attributes
                for key, value in attributes.items():
                    att_dict[key] = value

                payload = PubsubMessage(
                    received_message.message.data, att_dict, received_message.ack_id
                )
            elif received_message.message.data:
                payload = received_message.message.data
            else:
                logging.error(
                    "Received empty message from pubsub"
                    "did you mean to set include attributes?"
                )
                payload = None
            payloads.append(payload)
            ack_ids.append(received_message.ack_id)

        return PullResponse(payloads, _PubsubAckInfo(ack_ids))

    async def ack(self, ack_info: _PubsubAckInfo, success: bool):
        if ack_info.ack_ids:
            if success:
                await self.subscriber_client.acknowledge(
                    ack_ids=ack_info.ack_ids, subscription=self.subscription_id
                )
            else:
                # This nacks the messages. See:
                # https://github.com/googleapis/python-pubsub/pull/123/files
                ack_deadline_seconds = 0
                await self.subscriber_client.modify_ack_deadline(
                    subscription=self.subscription_id,
                    ack_ids=ack_info.ack_ids,
                    ack_deadline_seconds=ack_deadline_seconds,
                )

    async def backlog(self) -> int:
        split_sub = self.subscription_id.split("/")
        project = split_sub[1]
        sub_id = split_sub[3]
        # TODO: Create a gcp metrics utility library
        backlog_query = query.Query(
            client=self.metrics_client,
            project=project,
            end_time=datetime.datetime.now(),
            metric_type=(
                "pubsub.googleapis.com/subscription" "/num_unacked_messages_by_region"
            ),
            minutes=5,
        )
        backlog_query = backlog_query.select_resources(subscription_id=sub_id)
        last_timeseries = None
        try:
            for backlog_data in backlog_query.iter():
                last_timeseries = backlog_data
            if last_timeseries is None:
                return -1
        except Exception:
            logging.exception(
                "Failed to get backlog for subscription %s please ensure your "
                "user has: roles/monitoring.viewer to read the backlog, "
                "no autoscaling will happen.",
                self.subscription_id,
            )
            return -1
        points = list(last_timeseries.points)
        points.sort(
            key=lambda p: _timestamp_to_datetime(p.interval.end_time), reverse=True
        )
        return points[0].value.int64_value

    def max_batch_size(self) -> int:
        return self.batch_size

    def pull_converter(self, type_: Optional[Type]) -> Callable[[bytes], Any]:
        if type_ is None or self.include_attributes:
            # If include attributes is true, we always return a PubsubMessage
            return converters.identity()
        elif hasattr(type_, "from_bytes"):
            return lambda output: type_.from_bytes(output)
        elif dataclasses.is_dataclass(type_):
            return converters.bytes_to_dataclass(type_)
        else:
            if hasattr(type_, "__origin__"):
                type_ = type_.__origin__
            if issubclass(type_, bytes):
                return converters.identity()
            elif issubclass(type_, dict):
                return converters.bytes_to_dict()
            else:
                raise exceptions.CannotConvertSourceException(
                    f"Cannot convert from bytes to type: `{type_}`"
                )


class GCPPubSubTopicSink(SinkStrategy):
    def __init__(
        self,
        *,
        credentials: GCPCredentials,
        project_id: GCPProjectID,
        topic_name: PubSubTopicName,
    ):
        super().__init__(credentials=credentials, strategy_id="gcp-pubsub-topic-sink")
        self.project_id = project_id
        self.topic_name = topic_name
        clients = gcp_clients.GCPClients(
            credentials=credentials,
            quota_project_id=project_id,
        )
        self.publisher_client = clients.get_async_publisher_client()

    @property
    def topic_id(self) -> PubSubTopicID:
        return f"projects/{self.project_id}/topics/{self.topic_name}"

    @utils.log_errors(endpoint="apis.buildflow.dev/...")
    async def push(self, batch: Batch):
        pubsub_messages = [GCPPubSubMessage(data=elem) for elem in batch]
        await self.publisher_client.publish(
            topic=self.topic_id, messages=pubsub_messages
        )

    def push_converter(self, user_defined_type: Type) -> Callable[[Any], bytes]:
        return converters.bytes_push_converter(user_defined_type)
