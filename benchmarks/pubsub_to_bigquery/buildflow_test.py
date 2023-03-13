from google.cloud import bigquery
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import ray
import json


def json_rows_streaming(json_rows, bq_client):
    print(f'writing {len(json_rows)} rows to bq')
    errors = bq_client.insert_rows_json(
        'daring-runway-374503.taxi_ride_benchmark.buildflow', json_rows)
    if errors:
        raise RuntimeError(f'BigQuery streaming insert failed: {errors}')


@ray.remote
class Actor:

    def __init__(self):
        self.project_id = ''
        self.subscription_id = ''
        self.batch = []
        self.count = 0

    def run(self):
        bq_client = bigquery.Client()
        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = 'projects/daring-runway-374503/subscriptions/buildflow-sub'

        def callback(message: pubsub_v1.subscriber.message.Message) -> None:
            self.count += 1

            if self.count % 10_000 == 0:
                self.count = 0
                print(f'count: {self.count}')

            decoded_data = message.data.decode()
            json_loaded = json.loads(decoded_data)

            self.batch.append(json_loaded)

            if len(self.batch) == 5000:
                to_write = self.batch[:5000]
                self.batch = self.batch[5000:]
                json_rows_streaming(to_write, bq_client)

            message.ack()

        streaming_pull_future = subscriber.subscribe(subscription_path,
                                                     callback=callback)
        print(f"Listening for messages on {subscription_path}..\n")

        with subscriber:
            try:

                streaming_pull_future.result()
            except TimeoutError:
                streaming_pull_future.cancel()
                streaming_pull_future.result()


actors = [Actor.remote() for _ in range(100)]

ray.get([actor.run.remote() for actor in actors])
