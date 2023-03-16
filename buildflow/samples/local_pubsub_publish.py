import argparse
import os
import json
import sys

from google.cloud import pubsub_v1

parser = argparse.ArgumentParser()
parser.add_argument('--value', type=int, required=True)
args, _ = parser.parse_known_args(sys.argv)

if 'PUBSUB_EMULATOR_HOST' not in os.environ:
    # If this variable wasn't set. Set it to the same value we set in the
    # walkthrough docs.
    os.environ['PUBSUB_EMULATOR_HOST'] = 'localhost:8085'

topic = 'projects/local-buildflow-example/topics/my-topic'
client = pubsub_v1.PublisherClient()
topics = []
print('Publishing to: ', topic)
future = client.publish(
    topic,
    json.dumps({
        'value_field': args.value
    }).encode('UTF-8'))
future.result()
