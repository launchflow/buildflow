# flake8: noqa
import argparse
import json
import sys
from typing import Any, Dict

import buildflow
from buildflow import Flow


# Parser to allow run time configuration of arguments
parser = argparse.ArgumentParser()
parser.add_argument('--queue_name', type=str, required=True)
parser.add_argument('--file_path',
                    type=str,
                    default='/tmp/buildflow/local_pubsub.parquet')
args, _ = parser.parse_known_args(sys.argv)


input_sqs = buildflow.SQSSource(queue_name=args.queue_name)
sink = buildflow.FileSink(file_path=args.file_path,
                          file_format=buildflow.FileFormat.PARQUET)

flow = Flow()


@flow.processor(source=input_sqs, sink=sink)
def process(element: Dict[str, Any]):
    return json.loads(element['Body'])


# Run our flow.
flow.run().output()