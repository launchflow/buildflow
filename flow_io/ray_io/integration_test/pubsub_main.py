from google.cloud import pubsub_v1
import ray
from flow_io import ray_io

pubsub_project = 'pubsub-test-project'
ps_client = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
topic_path = ps_client.topic_path(pubsub_project, 'incoming_topic')
topic = ps_client.create_topic(request={"name": topic_path})
subscription_path = subscriber.subscription_path(pubsub_project,
                                                 'pubsub_main')
with subscriber:
    subscription = subscriber.create_subscription(request={
        "name": subscription_path,
        "topic": topic_path
    })

print('Creating the following: ')
print(' - topic: ', topic_path)
print(' - subscription: ', subscription_path)

topic_path = ps_client.topic_path(pubsub_project, 'outgoing_topic')
topic = ps_client.create_topic(request={"name": topic_path})
outgoing_topic = topic.name
subscription_path = subscriber.subscription_path(pubsub_project,
                                                 'validation')
# Wrap the subscriber in a 'with' block to automatically call close()
# to close the underlying gRPC channel when done.
subscriber = pubsub_v1.SubscriberClient()
with subscriber:
    subscription = subscriber.create_subscription(request={
        "name": subscription_path,
        "topic": topic_path
    })

print(' - topic: ', topic_path)
print(' - subscription: ', subscription_path)


@ray.remote
class ProcessorActor:

    def __init__(self, sink):
        self.sink = sink

    def process(self, ray_input, carrier):
        ray.get(
            sink.write.remote({'new_field': ray_input['field'] + 1}, carrier))


sink = ray_io.sink()
processor = ProcessorActor.remote(sink)
source = ray_io.source(processor.process)
ray.get(source.run.remote())
