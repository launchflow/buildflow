"""IO connectors for Pub/Sub and Ray."""

from typing import Any, Dict

import ray
from google.cloud import storage

from buildflow.api import io
from buildflow.runtime.ray_io import base

from google.pubsub_v1.services.subscriber import SubscriberAsyncClient


@ray.remote
class GCSFileEventStreamSourceActor(base.RaySource):

    def __init__(
        self,
        ray_sinks: Dict[str, base.RaySink],
        gcs_events_ref: io.GCSFileEventStream,
    ) -> None:
        super().__init__(ray_sinks)
        self.pubsub_subscription = gcs_events_ref.pubsub_subscription
        self.batch_size = 1000
        self.running = True
        self._pending_ack_ids = []

    @staticmethod
    def recommended_num_threads():
        # The actor becomes mainly network bound after roughly 4 threads, and
        # additoinal threads start to hurt cpu utilization.
        # This number is based on a single actor instance.
        return 4

    async def run(self):
        pubsub_client = SubscriberAsyncClient()
        storage_client = storage.Client()
        while self.running:
            response = await pubsub_client.pull(
                subscription=self.pubsub_subscription,
                max_messages=self.batch_size)
            ack_ids = []
            payloads = []
            for received_message in response.received_messages:
                attributes = received_message.message.attributes
                event_type = attributes["eventType"]
                bucket_id = attributes["bucketId"]
                object_id = attributes["objectId"]

                payloads.append((event_type, bucket_id, object_id))
                ack_ids.append(received_message.ack_id)
                self._pending_ack_ids.append(ack_ids)

            # payloads will be empty if the pull times out (usually because
            # there's no data to pull).
            if payloads:
                await self._send_batch_to_sinks_and_await(payloads)
                # TODO: Add error handling.
                await pubsub_client.acknowledge(
                    ack_ids=ack_ids, subscription=self.pubsub_subscription)

    @staticmethod
    def preprocess(payload: Any):
        storage_client = storage.Client()
        storage_object = storage_client.get_bucket(bucket_id).get_blob(
            object_id)
        pass

    def shutdown(self):
        self.running = False
        return True
