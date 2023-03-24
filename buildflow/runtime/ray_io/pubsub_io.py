"""IO connectors for Pub/Sub and Ray."""

import asyncio
import dataclasses
from google.cloud import monitoring_v3
import inspect
import logging
import json
from typing import Any, Callable, Dict, Iterable, Optional, Union

from google.cloud import pubsub
from google.pubsub_v1.services.subscriber import SubscriberAsyncClient
import ray

from buildflow.api import io
from buildflow.runtime.ray_io import base
from buildflow.runtime.ray_io import pubsub_utils

_BACKLOG_QUERY_TEMPLATE = """\
fetch pubsub_subscription
| metric 'pubsub.googleapis.com/subscription/num_unacked_messages_by_region'
| filter
    resource.project_id == '{project}'
    && (resource.subscription_id == '{sub_id}')
| group_by 1m,
    [value_num_unacked_messages_by_region_mean:
       mean(value.num_unacked_messages_by_region)]
| every 1m
| group_by [],
    [value_num_unacked_messages_by_region_mean_aggregate:
       aggregate(value_num_unacked_messages_by_region_mean)]
"""


@dataclasses.dataclass(frozen=True)
class PubsubMessage:
    data: Dict[str, Any]
    attributes: Dict[str, Any]


@dataclasses.dataclass
class PubSubSource(io.StreamingSource):
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

    def backlog(self) -> Optional[int]:
        split_sub = self.subscription.split('/')
        project = split_sub[1]
        sub_id = split_sub[3]
        client = monitoring_v3.QueryServiceClient()
        result = client.query_time_series(
            request=monitoring_v3.QueryTimeSeriesRequest(
                name=f'projects/{project}',
                query=_BACKLOG_QUERY_TEMPLATE.format(project=project,
                                                     sub_id=sub_id),
                page_size=1))
        result_list = list(result)
        if result_list:
            # TODO: clean this up
            return result_list[0].point_data[0].values[0].double_value
        return None

    @classmethod
    def recommended_num_threads(cls):
        # The actor becomes mainly network bound after roughly 4 threads, and
        # additional threads start to hurt cpu utilization.
        # This number is based on a single actor instance.
        return 4


@dataclasses.dataclass
class PubSubSink(io.Sink):
    """Source for writing to a Pub/Sub topic."""

    topic: str

    def setup(self, process_arg_spec: inspect.FullArgSpec):
        pubsub_utils.maybe_create_topic(self.topic)

    def actor(self, remote_fn: Callable, is_streaming: bool):
        return PubSubSinkActor.remote(remote_fn, self)


@ray.remote(num_cpus=PubSubSource.num_cpus())
class PubSubSourceActor(base.StreamingRaySource):

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
            ack_ids = []
            payloads = []
            try:
                response = await pubsub_client.pull(
                    subscription=self.subscription,
                    max_messages=self.batch_size,
                    return_immediately=True)
            except Exception as e:
                logging.error('pubsub pull failed with: %s', e)
                continue
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
                    payload = PubsubMessage(json_loaded, att_dict)
                else:
                    payload = json_loaded
                payloads.append(payload)
                ack_ids.append(received_message.ack_id)

            # payloads will be empty if the pull times out (usually because
            # there's no data to pull).
            if payloads:
                try:
                    await self._send_batch_to_sinks_and_await(payloads)
                    await pubsub_client.acknowledge(
                        ack_ids=ack_ids, subscription=self.subscription)
                except Exception as e:
                    logging.error(('Failed to process message, '
                                   'will not be acked: error: %s'), e)
                    # This nacks the messages. See:
                    # https://github.com/googleapis/python-pubsub/pull/123/files
                    ack_deadline_seconds = 0
                    pubsub_client.modify_ack_deadline(
                        subscription=self.subscription,
                        ack_ids=ack_ids,
                        ack_deadline_seconds=ack_deadline_seconds)
            else:
                # This happens when we didn't get any messages.
                await asyncio.sleep(3)
            # For pub/sub we determine the utilization based on the number of
            # messages received versus how many we received.
            self.update_metrics(len(payloads))

    def shutdown(self):
        self.running = False
        print('Shutting down Pub/Sub subscription')
        return True


# TODO: put more though into this resource requirement
@ray.remote(num_cpus=.25)
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
