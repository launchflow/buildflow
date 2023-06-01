import dataclasses

import buildflow

app = buildflow.ComputeNode()


@dataclasses.dataclass
class Output:
    output_val: int


class MyProcessor(buildflow.Processor):
    @classmethod
    def source(cls):
        return buildflow.GCPPubSubSource(
            subscription=("projects/pubsub-test-project/subscriptions/pubsub_main"),
            topic="projects/pubsub-test-project/topics/incoming_topic",
        )

    @classmethod
    def sink(cls):
        return buildflow.GCPPubSubSink(
            topic="projects/pubsub-test-project/topics/outgoing_topic"
        )

    def process(self, payload: int) -> Output:
        return Output(payload["val"] + 1)


app.add_processor(MyProcessor())
