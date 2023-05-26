# Users can connect their processor to a stream source.

from dataclasses import dataclass

from buildflow import ComputeNode
from buildflow.io import PubSub

app = ComputeNode()


@dataclass
class InputMessage:
    field: str


@dataclass
class OutputMessage:
    field: str


# Compare to the FileUpload example in launchflow_provider.py.
@app.processor(
    source=PubSub(topic="projects/my-project/topics/my-topic1"),
    sink=PubSub(topic="projects/my-project/topics/my-topic2"),
)
def process_stream(message: InputMessage) -> OutputMessage:
    # TODO: Add logic to process the message.
    return OutputMessage(field=message.field)
