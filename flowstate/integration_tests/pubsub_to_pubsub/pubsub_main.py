import ray
from google.cloud import pubsub_v1

import easy_flow
from easy_flow import io

pubsub_project = 'pubsub-test-project'
ps_client = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
topic_path = ps_client.topic_path(pubsub_project, 'incoming_topic')
topic = ps_client.create_topic(request={"name": topic_path})
input_subscription_path = subscriber.subscription_path(pubsub_project,
                                                       'pubsub_main')
with subscriber:
    subscription = subscriber.create_subscription(request={
        "name": input_subscription_path,
        "topic": topic_path
    })

print('Creating the following: ')
print(' - topic: ', topic_path)
print(' - subscription: ', input_subscription_path)

output_topic_path = ps_client.topic_path(pubsub_project, 'outgoing_topic')
topic = ps_client.create_topic(request={"name": output_topic_path})
outgoing_topic = topic.name
subscription_path = subscriber.subscription_path(pubsub_project, 'validation')
# Wrap the subscriber in a 'with' block to automatically call close()
# to close the underlying gRPC channel when done.
subscriber = pubsub_v1.SubscriberClient()
with subscriber:
    subscription = subscriber.create_subscription(request={
        "name": subscription_path,
        "topic": output_topic_path
    })

print(' - topic: ', output_topic_path)
print(' - subscription: ', subscription_path)

easy_flow.init(
    config={
        'input': easy_flow.PubSub(subscription=input_subscription_path),
        'outputs': [easy_flow.PubSub(topic=output_topic_path)]
    })


@ray.remote
class ProcessorActor:

    def process(self, payload: int):
        payload['val'] += 1
        return payload


processor = ProcessorActor.remote()
io.run(processor.process.remote)
