import dataclasses
import json
import logging
from typing import Any, Dict, List, Tuple

from buildflow.io.providers import PullProvider
from buildflow.io.providers.gcp import clients as gcp_clients


@dataclasses.dataclass(frozen=True)
class PubsubMessage():
    data: Dict[str, Any]
    attributes: Dict[str, Any]

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__)


class GCPPubSubProvider(PullProvider):

    def __init__(self,
                 *,
                 billing_project_id: str,
                 subscription_id: str,
                 batch_size: str,
                 include_attributes: bool = False):
        super().__init__()
        # configuration
        self.subscription_id = subscription_id
        self.batch_size = batch_size
        self.include_attributes = include_attributes
        # setup
        self.subscriber_client = gcp_clients.get_async_subscriber_client(
            billing_project_id)
        self.publisher_client = gcp_clients.get_publisher_client(
            billing_project_id)
        # initial state

    async def pull(self) -> Tuple[List[dict], List[str]]:
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
            # if self.include_attributes:
            #     att_dict = {}
            #     attributes = received_message.message.attributes
            #     for key, value in attributes.items():
            #         att_dict[key] = value
            #     payload = PubsubMessage(json_loaded, att_dict)

            payload = json_loaded
            payloads.append(payload)
            ack_ids.append(received_message.ack_id)

        return payloads, ack_ids

    async def ack(self, ack_ids: List[str]):
        if ack_ids:
            await self.subscriber_client.acknowledge(
                ack_ids=ack_ids, subscription=self.subscription_id)
