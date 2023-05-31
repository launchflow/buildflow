import asyncio
from dataclasses import asdict, dataclass
import datetime
import json
import logging
from typing import Any, Dict, List, Optional, Tuple, Union

from google.cloud.monitoring_v3 import query

from buildflow.io.providers import (
    PlanProvider,
    PullProvider,
    SetupProvider,
    PushProvider,
)
from buildflow.io.providers.base import Batch
from buildflow.io.providers.gcp.utils import clients as gcp_clients
from buildflow.io.providers.gcp.utils import setup_utils


@dataclass(frozen=True)
class _PubSubSourcePlan:
    topic_id: str
    subscription_id: str


@dataclass(frozen=True)
class PubsubMessage:
    data: Dict[str, Any]
    attributes: Dict[str, Any]

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__)


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

    async def push(self, batch: Batch):
        coros = []
        for elem in batch:
            coros.append(
                self.publisher_client.publish(
                    self.topic, json.dumps(elem).encode("UTF-8")
                )
            )
        await asyncio.gather(*coros)

    async def pull(self) -> Tuple[List[Union[dict, PubsubMessage]], List[str]]:
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
            json_loaded = {}
            if received_message.message.data:
                decoded_data = received_message.message.data.decode()
                json_loaded = json.loads(decoded_data)
                payload = json_loaded
            if self.include_attributes:
                att_dict = {}
                attributes = received_message.message.attributes
                for key, value in attributes.items():
                    att_dict[key] = value
                payload = PubsubMessage(json_loaded, att_dict)

            payloads.append(payload)
            ack_ids.append(received_message.ack_id)

        return payloads, ack_ids

    async def ack(self, ack_ids: List[str]):
        if ack_ids:
            await self.subscriber_client.acknowledge(
                ack_ids=ack_ids, subscription=self.subscription_id
            )

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
