"""IO connectors for Pub/Sub and Ray."""

import dataclasses
import inspect
import json
from typing import Any, Callable, Dict, Iterable, Union

from google.api_core import exceptions
from google.cloud import pubsub
from google.pubsub_v1.services.subscriber import SubscriberAsyncClient
import ray

from buildflow.api import io
from buildflow.runtime.ray_io import base


@dataclasses.dataclass
class PubSubSource(io.Source):
    """Source for connecting to a Pub/Sub subscription."""

    subscription: str
    # The topic to connect to for the subscription. If this is provided and
    # subscription does not exist we will create it.
    topic: str = ''

    def setup(self):
        subscriber_client = pubsub.SubscriberClient()
        try:
            subscriber_client.get_subscription(
                subscription=self.subscription)
        except exceptions.NotFound:
            if not self.topic:
                raise ValueError(
                    f'subscription: {self.subscription} was not found, '
                    'please provide the topic so we can create the '
                    'subscriber or ensure you have read access to the '
                    'subscribe.')
            publisher_client = pubsub.PublisherClient()
            try:
                publisher_client.get_topic(topic=self.topic)
            except exceptions.NotFound:
                print(f'topic {self.topic} not found attempting to create')
                try:
                    print(f'Creating topic: {self.topic}')
                    publisher_client.create_topic(name=self.topic)
                except exceptions.PermissionDenied:
                    raise ValueError(
                        f'Failed to create topic: {self.topic}. Please ensure '
                        'you have permission to read the existing topic or '
                        'permission to create a new topic if needed.')
            try:
                print(f'Creating subscription: {self.subscription}')
                subscriber_client.create_subscription(
                    name=self.subscription, topic=self.topic)
            except exceptions.PermissionDenied:
                raise ValueError(
                    f'Failed to create subscription: {self.subscription}. '
                    'Please ensure you have permission to read the '
                    'existing subscription or permission to create a new '
                    'subscription if needed.')

    def actor(self, ray_sinks):
        return PubSubSourceActor.remote(ray_sinks, self)

    @classmethod
    def is_streaming(cls) -> bool:
        return True

    @classmethod
    def recommended_num_threads(cls):
        # The actor becomes mainly network bound after roughly 4 threads, and
        # additoinal threads start to hurt cpu utilization.
        # This number is based on a single actor instance.
        return 8


@dataclasses.dataclass
class PubSubSink(io.Sink):
    """Source for writing to a Pub/Sub topic."""

    topic: str

    def setup(self, process_arg_spec: inspect.FullArgSpec):
        publisher_client = pubsub.PublisherClient()
        try:
            publisher_client.get_topic(topic=self.topic)
        except exceptions.NotFound:
            print(f'topic {self.topic} not found attempting to create')
            try:
                print(f'Creating topic: {self.topic}')
                publisher_client.create_topic(name=self.topic)
            except exceptions.PermissionDenied:
                raise ValueError(
                    f'Failed to create topic: {self.topic}. Please ensure '
                    'you have permission to read the existing topic or '
                    'permission to create a new topic if needed.')

    def actor(self, remote_fn: Callable, is_streaming: bool):
        return PubSubSinkActor.remote(remote_fn, self)


@ray.remote
class PubSubSourceActor(base.RaySource):

    def __init__(
        self,
        ray_sinks: Dict[str, base.RaySink],
        pubsub_ref: PubSubSource,
    ) -> None:
        super().__init__(ray_sinks)
        self.subscription = pubsub_ref.subscription
        self.batch_size = 1000
        self.running = True
        self._pending_ack_ids = []

    async def run(self):
        pubsub_client = SubscriberAsyncClient()
        while self.running:
            response = await pubsub_client.pull(subscription=self.subscription,
                                                max_messages=self.batch_size)
            ack_ids = []
            payloads = []
            for received_message in response.received_messages:
                decoded_data = received_message.message.data.decode()
                json_loaded = json.loads(decoded_data)
                payloads.append(json_loaded)
                ack_ids.append(received_message.ack_id)
                self._pending_ack_ids.append(ack_ids)

            # payloads will be empty if the pull times out (usually because
            # there's no data to pull).
            if payloads:
                await self._send_batch_to_sinks_and_await(payloads)
                # TODO: Add error handling.
                await pubsub_client.acknowledge(ack_ids=ack_ids,
                                                subscription=self.subscription)

    def shutdown(self):
        self.running = False
        return True


@ray.remote
class PubSubSinkActor(base.RaySink):

    def __init__(
        self,
        remote_fn: Callable,
        pubsub_ref: PubSubSink,
    ) -> None:
        super().__init__(remote_fn)
        self.pubslisher_client = pubsub.PublisherClient()
        self.topic = pubsub_ref.topic

    async def _write(
        self,
        elements: Union[Iterable[Dict[str, Any]],
                        Iterable[Iterable[Dict[str, Any]]]],
    ):

        def publish_element(item):
            future = self.pubslisher_client.publish(
                self.topic,
                json.dumps(item).encode('UTF-8'))
            future.result()

        for elem in elements:
            if isinstance(elem, dict):
                publish_element(elem)
            else:
                for subelem in elem:
                    publish_element(subelem)
        return
