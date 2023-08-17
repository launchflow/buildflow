from buildflow.core.processor.processor import ProcessorAPI, ProcessorID, ProcessorType
from buildflow.io.endpoint import Endpoint


class EndpointProcessor(ProcessorAPI):
    processor_type = ProcessorType.ENDPOINT

    def __init__(self, processor_id: ProcessorID):
        self.processor_id = processor_id

    def endpoint(self) -> Endpoint:
        raise NotImplementedError("endpoint not implemented for CollectorProcessor")

    # This lifecycle method is called once per payload.
    def process(self, element, **kwargs):
        raise NotImplementedError("process not implemented for CollectorProcessor")
