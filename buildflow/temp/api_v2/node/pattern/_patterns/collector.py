# ruff: noqa: E501
from buildflow.api_v2.node.pattern import PatternAPI
from buildflow.api_v2.node.pattern.primitive._primitives.sink import Batch, Sink
from buildflow.api_v2.node.pattern.primitive._primitives.endpoint import (
    Endpoint,
    Request,
)


CollectorID = str


class Collector(PatternAPI):
    collector_id: CollectorID
    endpoint: Endpoint
    sink: Sink

    # This lifecycle method is called once per payload.
    def process(self, request: Request, **kwargs) -> Batch:
        raise NotImplementedError("process not implemented")
