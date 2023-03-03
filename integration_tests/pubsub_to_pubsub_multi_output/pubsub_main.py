from google.cloud import pubsub_v1

import buildflow

pubsub_project = 'pubsub-test-project'
ps_client = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
topic_path = ps_client.topic_path(pubsub_project, 'incoming_topic_multi')
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

output_topic_path = ps_client.topic_path(pubsub_project,
                                         'outgoing_topic_multi')
topic = ps_client.create_topic(request={"name": output_topic_path})
outgoing_topic = topic.name
subscription_path = subscriber.subscription_path(pubsub_project,
                                                 'validation_multi')
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


flow = buildflow.Flow()


class MyProcessor(buildflow.Processor):

    def _input():
        return buildflow.PubSub(subscription=input_subscription_path)

    def _output():
        return buildflow.PubSub(topic=output_topic_path)

    def process(self, payload: int):
        payload['val'] += 1
        return [payload, payload]


flow.run(MyProcessor)
