# ruff: noqa: E501
from buildflow.api_v2.node.pattern.pattern_api import PatternAPI
from buildflow.api_v2.node.pattern.primitive._primitives.sink import Batch, Sink
from buildflow.api_v2.node.pattern.primitive._primitives.source import (
    PullResponse,
    Source,
)

ProcessorID = str


class Processor(PatternAPI):
    processor_id: ProcessorID
    source: Source
    sink: Sink

    # This lifecycle method is called once per payload.
    def process(self, payload: PullResponse, **kwargs) -> Batch:
        raise NotImplementedError("process not implemented")
