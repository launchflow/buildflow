import json
import os
import time

from google.api_core import exceptions
from google.cloud import pubsub_v1


gcp_project = os.environ["GCP_PROJECT"]
validation_sub = os.environ["VALIDATION_SUB"]
incoming_topic = os.environ["INCOMING_TOPIC"]
outgoing_topic = os.environ["OUTGOING_TOPIC"]

# CREATE THE VALIDATION SUBSCRIPTION
# this is used out side of buildflow so we have to create it.
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(gcp_project, validation_sub)
# Wrap the subscriber in a 'with' block to automatically call close()
# to close the underlying gRPC channel when done.
subscriber = pubsub_v1.SubscriberClient()
while True:
    try:
        subscription = subscriber.create_subscription(
            request={
                "name": subscription_path,
                "topic": f"projects/{gcp_project}/topics/{outgoing_topic}",
            }
        )
        break
    except exceptions.NotFound:
        time.sleep(2)

_TIMEOUT_SECS = 60

topic = f"projects/{gcp_project}/topics/{incoming_topic}"
client = pubsub_v1.PublisherClient()
topics = []
start_time = time.time()
while topic not in topics:
    topics = list(client.list_topics(project=f"projects/{gcp_project}"))
    topics = [t.name for t in topics]
    if time.time() - start_time > _TIMEOUT_SECS:
        raise ValueError(f"Unable to find topic: {topic}")
    time.sleep(1)

print("Publishing to: ", topic)
future = client.publish(topic, json.dumps({"val": 1}).encode("UTF-8"))
future.result()
