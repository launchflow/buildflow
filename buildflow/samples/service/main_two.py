from dataclasses import dataclass

from buildflow import Flow
from buildflow.core.dependencies.dependency import Client
from buildflow.io.gcp import GCPPubSubTopic
from buildflow.io.local import File, Pulse


@dataclass
class InputRequest:
    val: int


@dataclass
class OuptutResponse:
    val: int


pubsub_topic = GCPPubSubTopic(
    project_id="daring-runway-374503", topic_name="tanke-test-topic"
)


app = Flow()


@app.pipeline(source=Pulse([InputRequest(1)], 1), sink=File("output.txt", "csv"))
def my_pipeline_processor(
    input: InputRequest, pubsub_client=Client(pubsub_topic)
) -> OuptutResponse:
    return OuptutResponse(val=input.val + 1)
