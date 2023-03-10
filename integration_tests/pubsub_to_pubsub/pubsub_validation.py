import json
from concurrent.futures import TimeoutError

from google.cloud import pubsub_v1

output_subscriber = 'projects/pubsub-test-project/subscriptions/validation'
expected_data = {'output_val': 2}

match = False


def callback(message):
    global match
    try:
        got = json.loads(message.data.decode())
        assert expected_data == got, f'expected: {expected_data} got: {got}'
        match = True
    finally:
        message.ack()


with pubsub_v1.SubscriberClient() as subscriber:
    future = subscriber.subscribe(output_subscriber, callback=callback)
    print('Subscribing to: ', output_subscriber)
    try:
        future.result(timeout=20)
    except TimeoutError:
        print('Reached validation subscriber timeout.')

assert match, 'no match found from output topic.'
