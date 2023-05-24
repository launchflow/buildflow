import buildflow

app = buildflow.Node()


class MyProcessor(buildflow.Processor):
    @classmethod
    def source(self):
        return buildflow.GCPPubSubSource(
            subscription=("projects/pubsub-test-project/subscriptions/pubsub_multi"),
            topic="projects/pubsub-test-project/topics/incoming_topic_multi",
        )

    @classmethod
    def sink(self):
        return buildflow.GCPPubSubSink(
            topic="projects/pubsub-test-project/topics/outgoing_topic_multi"
        )

    def process(self, payload: int):
        payload["val"] += 1
        return [payload, payload]


app.add_processor(MyProcessor())
