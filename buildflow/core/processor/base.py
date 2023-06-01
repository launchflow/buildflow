from buildflow.api import ProcessorAPI, SinkType, SourceType
from buildflow.io.registry import EmptySink


class Processor(ProcessorAPI):
    def __init__(self, name: str = "") -> None:
        self.name = name

    @classmethod
    def source(cls) -> SourceType:
        raise NotImplementedError("source not implemented")

    @classmethod
    def sink(self) -> SinkType:
        return EmptySink()

    def process(self, payload, **kwargs):
        return payload
