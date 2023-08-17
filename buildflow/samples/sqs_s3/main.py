import os
from dataclasses import dataclass

import buildflow
from buildflow.io.aws import S3Bucket, SQSQueue
from buildflow.types.portable import FileFormat

bucket = os.environ["BUCKET"]
queue_name = os.environ["SQS_QUEUE"]

app = buildflow.Flow(flow_options=buildflow.FlowOptions(require_confirmation=False))

source = SQSQueue(queue_name, aws_region="us-east-1").options(managed=True)
sink = S3Bucket(
    bucket_name=bucket,
    file_format=FileFormat.CSV,
    file_path="output.csv",
    aws_region="us-east-1",
).options(
    managed=True,
    force_destroy=True,
)


@dataclass
class Input:
    val: int


@dataclass
class Output:
    output_val: int


@app.pipeline(source=source, sink=sink)
class MyProcessor:
    def process(self, payload: Input) -> Output:
        return Output(output_val=payload.val + 1)
