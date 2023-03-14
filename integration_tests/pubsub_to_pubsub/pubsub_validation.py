import json
from concurrent.futures import TimeoutError

from google.cloud import pubsub_v1


# CREATE THE VALIDATION SUBSCRIPTION
# this is used out side of buildflow so we have to create it.
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path('pubsub-test-project',
                                                 'validation')

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
    future = subscriber.subscribe(subscription_path, callback=callback)
    print('Subscribing to: ', subscription_path)
    try:
        future.result(timeout=20)
    except TimeoutError:
        print('Reached validation subscriber timeout.')

assert match, 'no match found from output topic.'
