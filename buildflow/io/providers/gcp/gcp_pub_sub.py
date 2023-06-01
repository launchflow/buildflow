import asyncio
from dataclasses import asdict, dataclass, is_dataclass
import datetime
import json
import logging
from typing import Any, Callable, Dict, Iterable, Optional, Type

from google.cloud.monitoring_v3 import query

from buildflow.io.providers import (
    PlanProvider,
    PullProvider,
    SetupProvider,
    PushProvider,
)
from buildflow.io.providers.base import PullResponse, AckInfo
from buildflow.io.providers.gcp.utils import clients as gcp_clients
from buildflow.io.providers.gcp.utils import setup_utils
from buildflow.io.providers.schemas import converters


@dataclass(frozen=True)
class _PubSubSourcePlan:
    topic_id: str
    subscription_id: str


@dataclass(frozen=True)
class _PubsubAckInfo(AckInfo):
    ack_ids: Iterable[str]


@dataclass(frozen=True)
class PubsubMessage:
    data: bytes
    attributes: Dict[str, Any]


class GCPPubSubProvider(PullProvider, SetupProvider, PlanProvider, PushProvider):
    def __init__(
        self,
        *,
        billing_project_id: str,
        topic_id: str,
        subscription_id: str,
        batch_size: str,
        include_attributes: bool = False,
    ):
        super().__init__()
        # configuration
        self.billing_project_id = billing_project_id
        self.topic_id = topic_id
        self.subscription_id = subscription_id
        self.batch_size = batch_size
        self.include_attributes = include_attributes
        # setup
        self.subscriber_client = gcp_clients.get_async_subscriber_client(
            billing_project_id
        )
        self.publisher_client = gcp_clients.get_async_publisher_client(
            billing_project_id
        )
        # initial state

    async def push(self, batch: Iterable[Any]):
        coros = []
        for elem in batch:
            coros.append(
                self.publisher_client.publish(
                    self.topic, json.dumps(elem).encode("UTF-8")
                )
            )
        await asyncio.gather(*coros)

    async def pull(self) -> PullResponse:
        try:
            response = await self.subscriber_client.pull(
                subscription=self.subscription_id,
                max_messages=self.batch_size,
                return_immediately=True,
            )
        except Exception as e:
            logging.error("pubsub pull failed with: %s", e)
            return [], []

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

    def pull_converter(self, type_: Optional[Type]) -> Callable[[bytes], Any]:
        if type_ is None:
            return converters.identity()
        elif hasattr(type_, "from_bytes"):
            return lambda output: type_.from_bytes(output)
        elif is_dataclass(type_):
            return converters.bytes_to_dataclass(type_)
        elif issubclass(type_, bytes):
            return converters.identity()
        else:
            raise ValueError("Cannot convert from bytes to type: `{type_}`")

    def push_converter(self, type_: Optional[Type]) -> Callable[[Any], bytes]:
        if type_ is None:
            return converters.identity()
        elif hasattr(type_, "to_bytes"):
            return lambda output: type_.to_bytes(output)
        elif is_dataclass(type_):
            return converters.dataclass_to_bytes()
        elif issubclass(type_, bytes):
            return converters.identity()
        else:
            raise ValueError("Cannot convert from type to bytes: `{type_}`")

    # TODO: This should not be Optional (goes against Pullable base class)
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

    async def plan(self) -> Dict[str, Any]:
        plan_dict = asdict(
            _PubSubSourcePlan(
                topic_id=self.topic_id, subscription_id=self.subscription_id
            )
        )
        if not plan_dict["topic_id"]:
            del plan_dict["topic_id"]
        return plan_dict

    async def setup(self) -> bool:
        setup_utils.maybe_create_subscription(
            pubsub_subscription=self.subscription_id,
            pubsub_topic=self.topic_id,
            billing_project=self.billing_project_id,
        )
