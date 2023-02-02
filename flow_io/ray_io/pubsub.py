"""IO connectors for Pub/Sub and Ray."""

import json
from typing import Dict

from google.cloud import pubsub_v1
import ray


@ray.remote
class PubSubSourceActor:

    def __init__(
        self,
        ray_inputs,
        input_node: str,
        topic: str,
        subscriptions: Dict[str, str],
    ) -> None:
        self.ray_inputs = ray_inputs
        self.topic = topic
        try:
            self.subscription = subscriptions[input_node]
        except KeyError:
            raise ValueError(
                'Subscription was not available for incoming node: '
                f'{input_node}. Avaliable subscriptions: {subscriptions}')

    def run(self):
        while True:
            with pubsub_v1.SubscriberClient() as s:
                # TODO: make this configurable
                response = s.pull(subscription=self.subscription,
                                  max_messages=10)

                ack_ids = []
                for received_message in response.received_messages:
                    ack_ids.append(received_message.ack_id)
                    for ray_input in self.ray_inputs:
                        # TODO: maybe we should add the option to include the
                        # attributes. I believe beam provides that as an
                        # option.
                        decoded_data = received_message.message.data.decode()
                        ray.get(ray_input.remote(json.loads(decoded_data)))
                if ack_ids:
                    s.acknowledge(ack_ids=ack_ids,
                                  subscription=self.subscription)


@ray.remote
class PubsubSinkActor:

    def __init__(
        self,
        topic: str,
        subscriptions: Dict[str, str],
    ) -> None:
        self.pubslisher_client = pubsub_v1.PublisherClient()
        self.topic = topic

    async def write(self, element: Dict):
        future = self.pubslisher_client.publish(
            self.topic,
            json.dumps(element).encode('UTF-8'))
        future.result()
