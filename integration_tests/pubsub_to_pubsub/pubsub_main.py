import dataclasses

import buildflow


@dataclasses.dataclass
class Output:
    output_val: int


flow = buildflow.Flow()


class MyProcessor(buildflow.Processor):

    def source(self):
        return buildflow.PubSubSource(
            subscription=(
                'projects/pubsub-test-project/subscriptions/pubsub_main'),
            topic='projects/pubsub-test-project/topics/incoming_topic')

    def sink(self):
        return buildflow.PubSubSink(
            topic='projects/pubsub-test-project/topics/outgoing_topic')

    def process(self, payload: int) -> Output:
        return Output(payload['val'] + 1)


flow.run(MyProcessor()).output()
