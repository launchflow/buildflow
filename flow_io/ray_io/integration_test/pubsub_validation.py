import json
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError

output_subscriber = 'projects/pubsub-test-project/subscriptions/validation'
expected_data = {'new_field': 2}

match = False


def callback(message):
    global match
    try:
        decode = json.loads(message.data.decode())
        assert expected_data == decode
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
