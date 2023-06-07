import os
from dataclasses import dataclass

import buildflow


gcp_project = os.environ["GCP_PROJECT"]
outgoing_topic = os.environ["OUTGOING_TOPIC"]
incoming_topic = os.environ["INCOMING_TOPIC"]
main_sub = os.environ["MAIN_SUB"]

infra_config = buildflow.InfraConfig(
    schema_validation=buildflow.SchemaValidation.LOG_WARNING,
    require_confirmation=False,
    log_level="WARNING",
)

app = buildflow.Node(infra_config=infra_config)


@dataclass
class Input:
    val: int


@dataclass
class Output:
    output_val: int


class MyProcessor(buildflow.Processor):
    @classmethod
    def source(cls):
        return buildflow.io.GCPPubSubSubscription(
            project_id=gcp_project,
            subscription_name=main_sub,
            topic_id=f"projects/{gcp_project}/topics/{incoming_topic}",
        )

    @classmethod
    def sink(cls):
        return buildflow.io.GCPPubSubTopic(
            project_id=gcp_project,
            topic_name=outgoing_topic,
        )

    def process(self, payload: Input) -> Output:
        return Output(output_val=payload.val + 1)


app.add(MyProcessor())
