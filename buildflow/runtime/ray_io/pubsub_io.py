"""IO connectors for Pub/Sub and Ray."""

import json
from typing import Any, Callable, Dict, Iterable, Union

import ray
from google.cloud import pubsub_v1

from buildflow.api import resources
from buildflow.runtime.ray_io import base

from google.pubsub_v1.services.subscriber import SubscriberAsyncClient


@ray.remote
class PubSubSourceActor(base.RaySource):

    def __init__(
        self,
        ray_sinks: Dict[str, base.RaySink],
        pubsub_ref: resources.PubSub,
    ) -> None:
        super().__init__(ray_sinks)
        self.subscription = pubsub_ref.subscription
        self.batch_size = 1000

    @staticmethod
    def recommended_num_threads():
        # The actor becomes mainly network bound after roughly 4 threads, and
        # additoinal threads start to hurt cpu utilization.
        # This number is based on a single actor instance.
        return 8

    async def run(self):
        pubsub_client = SubscriberAsyncClient()
        while True:
            response = await pubsub_client.pull(subscription=self.subscription,
                                                max_messages=self.batch_size)
            ack_ids = []
            payloads = []
            for received_message in response.received_messages:
                decoded_data = received_message.message.data.decode()
                json_loaded = json.loads(decoded_data)
                payloads.append(json_loaded)
                ack_ids.append(received_message.ack_id)
            # payloads will be empty if the pull times out (usually because
            # there's no data to pull).
            if payloads:
                await self._send_batch_to_sinks_and_await(payloads)
                # TODO: Add error handling.
                await pubsub_client.acknowledge(ack_ids=ack_ids,
                                                subscription=self.subscription)


@ray.remote
class PubsubSinkActor(base.RaySink):

    def __init__(
        self,
        remote_fn: Callable,
        pubsub_ref: resources.PubSub,
    ) -> None:
        super().__init__(remote_fn)
        self.pubslisher_client = pubsub_v1.PublisherClient()
        self.topic = pubsub_ref.topic

    async def _write(
        self,
        elements: Union[Iterable[Dict[str, Any]],
                        Iterable[Iterable[Dict[str, Any]]]],
    ):

        def publish_dict(item):
            future = self.pubslisher_client.publish(
                self.topic,
                json.dumps(item).encode('UTF-8'))
            future.result()

        for elem in elements:
            if isinstance(elem, dict):
                publish_dict(elem)
            else:
                for subelem in elem:
                    publish_dict(subelem)
        return
