"""IO connectors for Pub/Sub and Ray."""

import json
from typing import Dict, Iterable

from google.cloud import pubsub_v1
import ray

from flow_io import utils


class PubSubInput:

    def __init__(
        self,
        ray_dags: Iterable,
        topic: str,
        subscriptions: Dict[str, str],
    ) -> None:
        self.ray_dags = ray_dags
        self.topic = topic
        incoming_node = utils._get_node_launch_file(3)
        self.subscription = None
        for node_space, subscription in subscriptions.items():
            if node_space in incoming_node:
                self.subscription = subscription
        if self.subscription is None:
            raise ValueError(
                'Subscription was not available for incoming node: '
                f'{incoming_node}')

    def run(self):

        def callback(message):
            for dag in self.ray_dags:
                decoded_data = json.loads(message.data.decode())
                ray.get(dag.execute(decoded_data))
            message.ack()

        with pubsub_v1.SubscriberClient() as subscriber:
            future = subscriber.subscribe(self.subscription, callback)
            future.result()


@ray.remote
class PubSubOutput:

    def __init__(
        self,
        topic: str,
        subscriptions: Dict[str, str],
    ) -> None:
        self.pubslisher_client = pubsub_v1.PublisherClient()
        self.topic = topic

    def write(self, element: Dict):
        future = self.pubslisher_client.publish(
            self.topic,
            json.dumps(element).encode('UTF-8'))
        future.result()
