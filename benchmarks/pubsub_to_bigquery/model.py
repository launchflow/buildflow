import json
import time
from concurrent.futures import TimeoutError
# from ray.util.queue import Queue
from queue import Queue

import ray
from google.cloud import bigquery, pubsub_v1


@ray.remote
class AsyncProcessor(object):

    def __init__(self, process_fn):
        self.process_fn = process_fn

        self.subscription_path = 'projects/daring-runway-374503/subscriptions/buildflow-sub'
        self.bq_table = 'daring-runway-374503.taxi_ride_benchmark.buildflow'

        self.read_queue = Queue()
        self.write_queue = Queue()

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