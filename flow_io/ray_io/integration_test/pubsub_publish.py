import json
import time

from google.cloud import pubsub_v1

_TIMEOUT_SECS = 60

topic = 'projects/pubsub-test-project/topics/incoming_topic'
client = pubsub_v1.PublisherClient()
topics = []
start_time = time.time()
while topic not in topics:
    topics = list(client.list_topics(project='projects/pubsub-test-project'))
    topics = [t.name for t in topics]
    if time.time() - start_time > _TIMEOUT_SECS:
        raise ValueError(f'Unable to find topic: {topic}')
    time.sleep(1)

print('Publishing to: ', topic)
future = client.publish(topic, json.dumps({'val': 1}).encode('UTF-8'))
future.result()
