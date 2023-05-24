"""Basic flow that reads from an AWS SQS queue and writes to a local parquet file.

This assumes you have set up AWS on your local machine.

See: https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sso.html
"""
import json
import os
from typing import Any, Dict

import buildflow
from buildflow import Node


queue_name = os.environ["QUEUE_NAME"]
region = os.environ.get("REGION", "us-east-1")
file_path = os.environ.get("OUTPUT_FILE_PATH", "/tmp/buildflow/local_pubsub.parquet")

source = buildflow.SQSSource(queue_name=queue_name, region=region, batch_size=1)
sink = buildflow.FileSink(file_path=file_path, file_format=buildflow.FileFormat.PARQUET)

flow = Node()


@flow.processor(source=source, sink=sink)
def process(element: Dict[str, Any]):
    return json.loads(element["Body"])


# Run our flow.
flow.run().output()
