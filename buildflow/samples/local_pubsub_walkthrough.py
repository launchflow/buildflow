# flake8: noqa
"""This walkthrough is inteded to be run locally using the Pub/Sub emulator..

Please follow our walkthrough on https://www.buildflow.dev/docs/category/walk-throughs
for how to run this.
"""
import argparse
import os
import sys
from typing import Any, Dict

import buildflow
from buildflow import Flow

# Parser to allow run time configuration of arguments
parser = argparse.ArgumentParser()
parser.add_argument('--file_path',
                    type=str,
                    default='/tmp/buildflow/local_pubsub.parquet')
args, _ = parser.parse_known_args(sys.argv)

if 'PUBSUB_EMULATOR_HOST' not in os.environ:
    # If this variable wasn't set. Set it to the same value we set in the
    # walkthrough docs.
    os.environ['PUBSUB_EMULATOR_HOST'] = 'localhost:8085'

# Set up a subscriber for the source.
# If this subscriber does not exist yet BuildFlow will create it.
input_sub = buildflow.PubSubSource(
    subscription=f'projects/local-buildflow-example/subscriptions/my-sub',
    topic=f'projects/local-buildflow-example/topics/my-topic')
# Set up a FileSink for writing to a file locally.
sink = buildflow.FileSink(file_path=args.file_path,
                          file_format=buildflow.FileFormat.PARQUET)

flow = Flow()


# Define our processor.
@flow.processor(source=input_sub, sink=sink)
def process(element: Dict[str, Any]):
    return element


# Run our flow.
flow.run().output()