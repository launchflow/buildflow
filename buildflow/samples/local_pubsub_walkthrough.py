"""This walkthrough is inteded to be run locally using the Pub/Sub emulator..

Please follow our walkthrough on https://www.buildflow.dev/docs/category/walk-throughs
for how to run this.
"""
import os
from typing import Any, Dict

import buildflow
from buildflow import ComputeNode

file_path = os.environ.get("OUTPUT_FILE_PATH",
                           "/tmp/buildflow/local_pubsub.parquet")

if "PUBSUB_EMULATOR_HOST" not in os.environ:
    # If this variable wasn't set. Set it to the same value we set in the
    # walkthrough docs.
    os.environ["PUBSUB_EMULATOR_HOST"] = "localhost:8085"

# Set up a subscriber for the source.
# If this subscriber does not exist yet BuildFlow will create it.
input_sub = buildflow.GCPPubSubSource(
    subscription="projects/local-buildflow-example/subscriptions/my-sub",
    topic="projects/local-buildflow-example/topics/my-topic",
)
# Set up a FileSink for writing to a file locally.
sink = buildflow.FileSink(file_path=file_path,
                          file_format=buildflow.FileFormat.PARQUET)

app = ComputeNode()


# Define our processor.
@app.processor(source=input_sub, sink=sink)
def process(element: Dict[str, Any]):
    return element


if __name__ == "__main__":
    app.run()
