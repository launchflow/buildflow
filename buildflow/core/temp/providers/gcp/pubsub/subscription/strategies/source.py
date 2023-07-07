import dataclasses
import datetime
import logging
from typing import Any, Callable, Dict, Iterable, Optional, Type

from google.cloud.monitoring_v3 import query

from buildflow import exceptions
from buildflow.core.io.providers.gcp import gcp_clients
from buildflow.core.io.schemas import converters
from buildflow.core.strategies.source import AckInfo, PullResponse, SourceStrategy


@dataclasses.dataclass(frozen=True)
class _PubsubAckInfo(AckInfo):
    ack_ids: Iterable[str]


@dataclasses.dataclass(frozen=True)
class PubsubMessage:
    data: bytes
    attributes: Dict[str, Any]


class GCPPubSubSubscriptionSource(SourceStrategy):
    def __init__(
        self,
        *,
        # common options
        subscription_name: str,
        topic_id: str,
        project_id: str,
        # io-only options
        batch_size: str,
        include_attributes: bool,
    ):
        super().__init__()
        # configuration
        self.subscription_name = subscription_name
        self.topic_id = topic_id
        self.project_id = project_id
        self.batch_size = batch_size
        self.include_attributes = include_attributes
        # setup
        self.subscriber_client = gcp_clients.get_async_subscriber_client(project_id)
        self.publisher_client = gcp_clients.get_async_publisher_client(project_id)

    @property
    def subscription_id(self):
        return f"projects/{self.project_id}/subscriptions/{self.subscription_name}"  # noqa: E501

    async def pull(self) -> PullResponse:
        try:
            response = await self.subscriber_client.pull(
                subscription=self.subscription_id,
                max_messages=self.batch_size,
                return_immediately=True,
            )
        except Exception as e:
            logging.error("pubsub pull failed with: %s", e)
            return PullResponse([], _PubsubAckInfo([]))

        payloads = []
        ack_ids = []
        for received_message in response.received_messages:
            if received_message.message.data:
                payload = received_message.message.data
            if self.include_attributes:
                att_dict = {}
                attributes = received_message.message.attributes
                for key, value in attributes.items():
                    att_dict[key] = value
                payload = PubsubMessage(received_message.message.data, att_dict)

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

    # TODO: This should not be Optional (goes against Pullable base class)
    # Should always return an int and handle the case where the backlog is 0
    async def backlog(self) -> Optional[int]:
        split_sub = self.subscription_id.split("/")
        project = split_sub[1]
        sub_id = split_sub[3]
        # TODO: Create a gcp metrics utility library
        client = gcp_clients.get_metrics_client(project)
        backlog_query = query.Query(
            client=client,
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
                return None
        except Exception:
            logging.error(
                "Failed to get backlog for subscription %s please ensure your "
                "user has: roles/monitoring.viewer to read the backlog, "
                "no autoscaling will happen.",
                self.subscription_id,
            )
            return None
        points = list(last_timeseries.points)
        points.sort(key=lambda p: p.interval.end_time, reverse=True)
        return points[0].value.int64_value

    def max_batch_size(self) -> int:
        return self.batch_size

    def pull_converter(self, type_: Optional[Type]) -> Callable[[bytes], Any]:
        if type_ is None:
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
