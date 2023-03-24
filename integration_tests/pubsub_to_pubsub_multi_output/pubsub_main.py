
import buildflow

flow = buildflow.Flow()


class MyProcessor(buildflow.Processor):

    def source(self):
        return buildflow.PubSubSource(
            subscription=(
                'projects/pubsub-test-project/subscriptions/pubsub_multi'),
            topic='projects/pubsub-test-project/topics/incoming_topic_multi')

    def sink(self):
        return buildflow.PubSubSink(
            topic='projects/pubsub-test-project/topics/outgoing_topic_multi')

    def process(self, payload: int):
        payload['val'] += 1
        return [payload, payload]


flow.run(MyProcessor()).output()
