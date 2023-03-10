import json
from concurrent.futures import TimeoutError

from google.cloud import pubsub_v1

# CREATE THE VALIDATION SUBSCRIPTION
# this is used out side of buildflow so we have to create it.
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path('pubsub-test-project',
                                                 'validation_multi')
# Wrap the subscriber in a 'with' block to automatically call close()
# to close the underlying gRPC channel when done.
subscriber = pubsub_v1.SubscriberClient()
with subscriber:
    subscription = subscriber.create_subscription(
        request={
            'name': subscription_path,
            'topic': 'projects/pubsub-test-project/topics/outgoing_topic_multi'
        })

expected_data = {'val': 2}

matches = 0


def callback(message):
    global matches
    try:
        decode = json.loads(message.data.decode())
        assert expected_data == decode
        matches += 1
    finally:
        message.ack()


with pubsub_v1.SubscriberClient() as subscriber:
    future = subscriber.subscribe(subscription_path, callback=callback)
    print('Subscribing to: ', subscription_path)
    try:
        future.result(timeout=20)
    except TimeoutError:
        print('Reached validation subscriber timeout.')

assert matches == 2, f'expected 2 matches got {matches}'
