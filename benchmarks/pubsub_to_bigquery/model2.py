import json
import time
from concurrent.futures import TimeoutError
# from ray.util.queue import Queue
from queue import Queue
from typing import Any, Callable, Dict, Iterable, Union

import ray
from google.cloud import bigquery, pubsub_v1
from google.pubsub_v1.services.subscriber import SubscriberAsyncClient

from buildflow.api import io
from buildflow.runtime.ray_io import base

import threading


@ray.remote
def read_bundle():
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
            self._pending_ack_ids.append(ack_ids)

        # payloads will be empty if the pull times out (usually because
        # there's no data to pull).
        if payloads:
            await self._send_batch_to_sinks_and_await(payloads)
            # TODO: Add error handling.
            await pubsub_client.acknowledge(ack_ids=ack_ids,
                                            subscription=self.subscription)


@ray.remote
class SourceActor(object):

    def __init__(self):
        self.subscription_path = 'projects/daring-runway-374503/subscriptions/buildflow-sub'

        self.batch_size = 5000
        self.batch = []

    def read(self):
        subscriber = pubsub_v1.SubscriberClient()
        flow_control = pubsub_v1.types.FlowControl(
            max_messages=self.batch_size * 2)

        def callback(message: pubsub_v1.subscriber.message.Message) -> None:
            payload = json.loads(message.data.decode())
            self.batch.append(payload)
            if len(self.batch) % self.batch_size == 0:
                attempt = 0
                while True:
                    attempt += 1
                    try:
                        _batch = self.batch[:self.batch_size]
                        self.read_queue.put(_batch)
                        break
                    except:
                        print('backing off')
                        time.sleep(1) * 2**attempt
                        continue
                self.batch = self.batch[self.batch_size:]
            message.ack()

        streaming_pull_future = subscriber.subscribe(self.subscription_path,
                                                     callback=callback,
                                                     flow_control=flow_control)
        print(f"Listening for messages on {self.subscription_path}..\n")

        with subscriber:
            try:
                streaming_pull_future.result()
            except TimeoutError:
                streaming_pull_future.cancel()
                streaming_pull_future.result()

    def process(self):
        while True:
            batch = self.read_queue.get()
            output = self.process_fn(batch)
            self.write_queue.put(output)

    def write(self):
        bq_client = bigquery.Client()
        while True:
            batch = self.write_queue.get()
            errors = bq_client.insert_rows_json(self.bq_table, batch)
            if errors:
                print('errors: ', errors)
                raise RuntimeError(
                    f'BigQuery streaming insert failed: {errors}')

            print(f'Finished writing {len(batch)} values to BigQuery.')


def my_process_fn(batch):
    return batch


num_read_thread = 1
num_process_threads = 1
num_write_threads = 1
num_replicas = 10

tasks = []
for _ in range(num_replicas):
    processor = AsyncProcessor.options(max_concurrency=3).remote(my_process_fn)
    tasks.extend([processor.read.remote() for _ in range(num_read_thread)])
    tasks.extend(
        [processor.process.remote() for _ in range(num_process_threads)])
    tasks.extend([processor.write.remote() for _ in range(num_write_threads)])

print('running tasks')
ray.get(tasks)
print('tasks done')