import os
from dataclasses import dataclass

import buildflow
from buildflow.io.gcp import GCPPubSubSubscription, GCPPubSubTopic, GCSBucket
from buildflow.types.portable import FileFormat

gcp_project = os.environ["GCP_PROJECT"]
incoming_topic = os.environ["INCOMING_TOPIC"]
main_sub = os.environ["MAIN_SUB"]
bucket = os.environ["BUCKET"]

app = buildflow.Flow(flow_options=buildflow.FlowOptions(require_confirmation=False))

source = GCPPubSubSubscription(
    project_id=gcp_project,
    subscription_name=main_sub,
).options(
    managed=True,
    topic=GCPPubSubTopic(project_id=gcp_project, topic_name=incoming_topic).options(
        managed=True
    ),
)
sink = GCSBucket(
    project_id=gcp_project,
    bucket_name=bucket,
    file_format=FileFormat.CSV,
    file_path="output.csv",
).options(
    managed=True,
    force_destroy=True,
    bucket_region="US",
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