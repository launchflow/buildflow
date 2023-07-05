import dataclasses

from buildflow.api.composites.sink import Sink
from buildflow.api.composites.source import Source
from buildflow.api.patterns._pattern import PatternAPI

ProcessorID = str


@dataclasses.dataclass
class Processor(PatternAPI):
    processor_id: ProcessorID

    def source(self) -> Source:
        raise NotImplementedError("source not implemented for Processor")

    def sink(self) -> Sink:
        raise NotImplementedError("sink not implemented for Processor")

    # This lifecycle method is called once per payload.
    def process(self, payload, **kwargs):
        raise NotImplementedError("process not implemented for Processor")
