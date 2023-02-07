"""IO connectors for Pub/Sub and Ray."""

import logging
import json
from typing import Any, Dict, Iterable, Union

from google.cloud import pubsub_v1
import ray

from flow_io.ray_io import base


@ray.remote
class PubSubSourceActor(base.RaySource):

    def __init__(
        self,
        ray_inputs,
        input_node: str,
        topic: str,
        subscriptions: Dict[str, str],
    ) -> None:
        super().__init__(ray_inputs, input_node)
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

                ray_futures = {}
                all_input_data = []
                for received_message in response.received_messages:
                    for ray_input in self.ray_inputs:
                        # TODO: maybe we should add the option to include the
                        # attributes. I believe beam provides that as an
                        # option.
                        decoded_data = received_message.message.data.decode()
                        json_loaded = json.loads(decoded_data)
                        all_input_data.append(json_loaded)
                        ref = ray_input.remote(json_loaded)
                        ray_futures[received_message.ack_id] = ref.future()

                if self.data_tracing_enabled:
                    base.add_to_span('input_data', {'field': 123})

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
        topic: str,
        subscriptions: Dict[str, str],
    ) -> None:
        super().__init__()
        self.pubslisher_client = pubsub_v1.PublisherClient()
        self.topic = topic

    async def _write(
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
