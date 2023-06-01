import dataclasses
import os

import buildflow


@dataclasses.dataclass
class Output:
    output_val: int


app = buildflow.ComputeNode()


class MyProcessor(buildflow.Processor):
    @classmethod
    def source(cls):
        return buildflow.GCPPubSubSource(
            subscription=("projects/pubsub-test-project/subscriptions/pubsub_main"),
            topic="projects/pubsub-test-project/topics/incoming_topic",
        )

    @classmethod
    def sink(cls):
        return buildflow.FileSink(
            file_path=os.environ["OUTPUT_FILE_PATH"],
            file_format=buildflow.FileFormat.PARQUET,
        )

    def process(self, payload: int) -> Output:
        return Output(payload["val"] + 1)


app.add_processor(MyProcessor())
