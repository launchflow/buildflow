import os
from dataclasses import dataclass

import buildflow
from buildflow.io.gcp import GCPPubSubSubscription, GCPPubSubTopic

gcp_project = os.environ["GCP_PROJECT"]
outgoing_topic = os.environ["OUTGOING_TOPIC"]
incoming_topic = os.environ["INCOMING_TOPIC"]
main_sub = os.environ["MAIN_SUB"]

app = buildflow.Flow(flow_options=buildflow.FlowOptions(require_confirmation=False))

incoming_topic = GCPPubSubTopic(project_id=gcp_project, topic_name=incoming_topic)
source = GCPPubSubSubscription(
    project_id=gcp_project, subscription_name=main_sub
).options(topic=incoming_topic)
sink = GCPPubSubTopic(project_id=gcp_project, topic_name=outgoing_topic)


app.manage(source, sink, incoming_topic)


@dataclass
class Input:
    val: int


@dataclass
class Output:
    output_val: int


@app.consumer(source=source, sink=sink)
class MyProcessor:
    def process(self, payload: Input) -> Output:
        return Output(output_val=payload.val + 1)
