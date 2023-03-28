"""Basic flow that reads from an AWS SQS queue and writes to a local parquet file.

This assumes you have set up AWS on your local machine.

See: https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sso.html
"""
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


source = buildflow.SQSSource(queue_name=args.queue_name)
sink = buildflow.FileSink(file_path=args.file_path,
                          file_format=buildflow.FileFormat.PARQUET)

flow = Flow()


@flow.processor(source=source, sink=sink)
def process(element: Dict[str, Any]):
    return json.loads(element['Body'])


# Run our flow.
flow.run().output()
