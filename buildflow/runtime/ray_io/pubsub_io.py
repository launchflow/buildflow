"""IO connectors for Pub/Sub and Ray."""

import dataclasses
import inspect
import json
from typing import Any, Callable, Dict, Iterable, Union

from google.cloud import pubsub
from google.pubsub_v1.services.subscriber import SubscriberAsyncClient
import ray

from buildflow.api import io
from buildflow.runtime.ray_io import base
from buildflow.runtime.ray_io import pubsub_utils


@dataclasses.dataclass(frozen=True)
class PubsubMessage:
    data: Dict[str, Any]
    attributes: Dict[str, Any]


@dataclasses.dataclass
class PubSubSource(io.Source):
    """Source for connecting to a Pub/Sub subscription."""

    subscription: str
    # The topic to connect to for the subscription. If this is provided and
    # subscription does not exist we will create it.
    topic: str = ''
    # Whether or not to include the pubsub attributes. If this is true you will
    # get a buildflow.PubsubMessage class as your input.
    include_attributes: bool = False

    def setup(self):
        pubsub_utils.maybe_create_subscription(self.subscription, self.topic)

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
        pubsub_utils.maybe_create_topic(self.topic)

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
        self.include_attributes = pubsub_ref.include_attributes
        self.batch_size = 1000
        self.running = True

    async def run(self):
        pubsub_client = SubscriberAsyncClient()
        while self.running:
            response = await pubsub_client.pull(subscription=self.subscription,
                                                max_messages=self.batch_size)
            ack_ids = []
            payloads = []
            for received_message in response.received_messages:
                json_loaded = {}
                if received_message.message.data:
                    decoded_data = received_message.message.data.decode()
                    json_loaded = json.loads(decoded_data)
                if self.include_attributes:
                    att_dict = {}
                    attributes = received_message.message.attributes
                    for key, value in attributes.items():
                        att_dict[key] = value
                    payload = PubsubMessage(
                        json_loaded, att_dict)
                else:
                    payload = json_loaded
                payloads.append(payload)
                ack_ids.append(received_message.ack_id)

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
        elements: Union[ray.data.Dataset, Iterable[Dict[str, Any]]],
    ):
        # TODO: need to support writing to Pub/Sub in batch mode.
        def publish_element(item):
            future = self.pubslisher_client.publish(
                self.topic,
                json.dumps(item).encode('UTF-8'))
            future.result()

        for elem in elements:
            publish_element(elem)
        return
