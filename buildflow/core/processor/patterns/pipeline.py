from buildflow.core.processor.processor import ProcessorAPI, ProcessorID, ProcessorType
from buildflow.core.strategies.sink import SinkStrategy
from buildflow.core.strategies.source import SourceStrategy


class PipelineProcessor(ProcessorAPI):
    processor_type = ProcessorType.PIPELINE

    def __init__(self, processor_id: ProcessorID):
        self.processor_id = processor_id

    def source(self) -> SourceStrategy:
        raise NotImplementedError("source not implemented for Pipeline")

    def sink(self) -> SinkStrategy:
        raise NotImplementedError("sink not implemented for Pipeline")

    # This lifecycle method is called once per payload.
    def process(self, element, **kwargs):
        raise NotImplementedError("process not implemented for Pipeline")
