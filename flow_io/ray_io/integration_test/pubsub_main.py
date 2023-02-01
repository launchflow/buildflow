from google.cloud import pubsub_v1
import ray
from ray.dag.input_node import InputNode
from flow_io import ray_io

pubsub_project = 'pubsub-test-project'
ps_client = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
topic_path = ps_client.topic_path(pubsub_project, 'incoming_topic')
topic = ps_client.create_topic(request={"name": topic_path})
subscription_path = subscriber.subscription_path(pubsub_project,
                                                 'flow_io-ray_io')
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
def ray_func(ray_input):
    return {'new_field': ray_input['field'] + 1}


with InputNode() as dag_input:
    output = ray_func.bind(dag_input)
    final_outputs = ray_io.output(output)

ray_io.input(*final_outputs)
