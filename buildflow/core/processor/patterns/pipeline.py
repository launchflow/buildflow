from buildflow.core.processor.processor import ProcessorAPI, ProcessorID, ProcessorType
from buildflow.io.strategies.sink import SinkStrategy
from buildflow.io.strategies.source import SourceStrategy


class PipelineProcessor(ProcessorAPI):
    processor_type = ProcessorType.PIPELINE

    def __init__(self, processor_id: ProcessorID):
        self.processor_id = processor_id

    def source(self) -> SinkStrategy:
        raise NotImplementedError("source not implemented for Pipeline")

    def sink(self) -> SourceStrategy:
        raise NotImplementedError("sink not implemented for Pipeline")

    # This lifecycle method is called once per payload.
    def process(self, element, **kwargs):
        raise NotImplementedError("process not implemented for Pipeline")
