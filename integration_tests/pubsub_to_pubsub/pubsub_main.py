import dataclasses

import buildflow

app = buildflow.Node()


@dataclasses.dataclass
class Output:
    output_val: int


class MyProcessor(buildflow.Processor):
    @classmethod
    def source(cls):
        return buildflow.io.GCPPubSubSubscription(
            subscription_id=("projects/pubsub-test-project/subscriptions/pubsub_main"),
            topic_id="projects/pubsub-test-project/topics/incoming_topic",
        )

    @classmethod
    def sink(cls):
        return buildflow.io.GCPPubSubTopic(
            topic_id="projects/pubsub-test-project/topics/outgoing_topic"
        )

    def process(self, payload: int) -> Output:
        return Output(payload["val"] + 1)


app.add(MyProcessor())
