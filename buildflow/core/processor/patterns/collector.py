from buildflow.core.processor.processor import ProcessorAPI, ProcessorID, ProcessorType
from buildflow.io.endpoint import Endpoint
from buildflow.io.strategies.sink import SinkStrategy


class CollectorProcessor(ProcessorAPI):
    processor_type = ProcessorType.COLLECTOR

    def __init__(self, processor_id: ProcessorID):
        self.processor_id = processor_id

    def endpoint(self) -> Endpoint:
        raise NotImplementedError("endpoint not implemented for CollectorProcessor")

    def sink(self) -> SinkStrategy:
        raise NotImplementedError("sink not implemented for CollectorProcessor")

    # This lifecycle method is called once per payload.
    def process(self, element, **kwargs):
        raise NotImplementedError("process not implemented for CollectorProcessor")
