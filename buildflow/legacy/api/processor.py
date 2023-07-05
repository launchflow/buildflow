from typing import Any, Iterable

from buildflow.api.io import SinkType, SourceType


ProcessorID = str


class ProcessorAPI:
    processor_id: ProcessorID

    # This lifecycle method defines the input reference for the processor.
    @classmethod
    def source(cls) -> SourceType:
        raise NotImplementedError("source not implemented")

    # This lifecycle method defines the output reference for the processor.
    @classmethod
    def sink(cls) -> SinkType:
        raise NotImplementedError("sink not implemented")

    # You can also define multiple outputs.
    @classmethod
    def sinks(self) -> Iterable[SinkType]:
        raise NotImplementedError("sinks not implemented")

    # This lifecycle method is called once per replica.
    def setup(self):
        raise NotImplementedError("setup not implemented")

    # This lifecycle method is called once per payload.
    def process(self, payload: Any, **kwargs):
        raise NotImplementedError("process not implemented")
