import argparse
import dataclasses
import sys

import buildflow

parser = argparse.ArgumentParser()
parser.add_argument('--file_path', type=str, required=True)
args, _ = parser.parse_known_args(sys.argv)


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
        return buildflow.FileSink(file_path=args.file_path,
                                  file_format=buildflow.FileFormat.PARQUET)

    def process(self, payload: int) -> Output:
        return Output(payload['val'] + 1)


flow.run(MyProcessor()).output()
