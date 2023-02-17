"""IO connectors for Pub/Sub and Ray."""

import json
import logging
from typing import Any, Callable, Dict, Iterable, Union

import ray
from google.cloud import pubsub_v1

from flow_io import resources
from flow_io.ray_io import base


@ray.remote
class PubSubSourceActor(base.RaySource):

    def __init__(
        self,
        ray_sinks: Iterable[base.RaySink],
        pubsub_ref: resources.PubSub,
    ) -> None:
        super().__init__(ray_sinks)
        self.subscription = pubsub_ref.subscription

    def run(self):
        while True:
            with pubsub_v1.SubscriberClient() as s:
                # TODO: make this configurable
                response = s.pull(subscription=self.subscription,
                                  max_messages=10)

                ray_futures = {}
                for received_message in response.received_messages:
                    # TODO: maybe we should add the option to include the
                    # attributes. I believe beam provides that as an
                    # option.
                    decoded_data = received_message.message.data.decode()
                    json_loaded = json.loads(decoded_data)
                    for ray_sink in self.ray_sinks:
                        # TODO: add tracing context
                        ref = ray_sink.write.remote(json_loaded)
                        ray_futures[received_message.ack_id] = ref.future()

                while ray_futures:
                    new_futures = {}
                    for ack_id, future in ray_futures.items():
                        if future.done():
                            try:
                                future.result()
                            except Exception as e:
                                logging.error(
                                    'Failed to process pubsub message. Message'
                                    ' has not been acked. Error : %s', e)
                            s.acknowledge(ack_ids=[ack_id],
                                          subscription=self.subscription)
                        else:
                            new_futures[ack_id] = future
                    ray_futures = new_futures


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

    def _write(
        self,
        element: Union[Dict[str, Any], Iterable[Dict[str, Any]]],
    ):
        to_insert = element
        if isinstance(element, dict):
            to_insert = [element]
        for item in to_insert:
            future = self.pubslisher_client.publish(
                self.topic,
                json.dumps(item).encode('UTF-8'))
            future.result()
