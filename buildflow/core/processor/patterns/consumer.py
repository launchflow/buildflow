from buildflow.core.processor.processor import (
    ProcessorAPI,
    ProcessorGroup,
    ProcessorGroupType,
    ProcessorID,
    ProcessorType,
)
from buildflow.io.strategies.sink import SinkStrategy
from buildflow.io.strategies.source import SourceStrategy


class ConsumerProcessor(ProcessorAPI):
    processor_type = ProcessorType.CONSUMER

    def __init__(self, processor_id: ProcessorID):
        self.processor_id = processor_id

    def source(self) -> SourceStrategy:
        raise NotImplementedError("source not implemented for Consumer")

    def sink(self) -> SinkStrategy:
        raise NotImplementedError("sink not implemented for Consumer")

    # This lifecycle method is called once per payload.
    def process(self, element, **kwargs):
        raise NotImplementedError("process not implemented for Consumer")


class ConsumerGroup(ProcessorGroup[ConsumerProcessor]):
    group_type: ProcessorGroupType = ProcessorGroupType.CONSUMER
