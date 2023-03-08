# Users can connect their processor to a stream source.

from dataclasses import dataclass

from buildflow import Flow
from buildflow.io import PubSub

flow = Flow()


@dataclass
class InputMessage:
    field: str


@dataclass
class OutputMessage:
    field: str


# Compare to the FileUpload example in launchflow_provider.py.
@flow.processor(
    source=PubSub(topic='projects/my-project/topics/my-topic1'),
    sink=PubSub(topic='projects/my-project/topics/my-topic2'),
)
def process_stream(message: InputMessage) -> OutputMessage:
    # TODO: Add logic to process the message.
    return OutputMessage(field=message.field)
