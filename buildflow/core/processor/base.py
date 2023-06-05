from buildflow.api import ProcessorAPI, SinkType, SourceType, ProcessorID
from buildflow.io.registry import EmptySink


class Processor(ProcessorAPI):
    def __init__(self, processor_id: ProcessorID = "") -> None:
        self.processor_id = processor_id

    @classmethod
    def source(cls) -> SourceType:
        raise NotImplementedError("source not implemented")

    @classmethod
    def sink(self) -> SinkType:
        return EmptySink()

    def setup(self):
        pass

    def process(self, payload, **kwargs):
        return payload
