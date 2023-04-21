from typing import Any, Iterable

from buildflow.api.io import SourceType, SinkType


class ProcessorAPI:

    # This lifecycle method defines the input reference for the processor.
    def source(self) -> SourceType:
        raise NotImplementedError('source not implemented')

    # This lifecycle method defines the output reference for the processor.
    def sink(self) -> SinkType:
        raise NotImplementedError('sink not implemented')

    # You can also define multiple outputs.
    def sinks(self) -> Iterable[SinkType]:
        raise NotImplementedError('sinks not implemented')

    # This lifecycle method initializes any shared state.
    def setup(self):
        raise NotImplementedError('setup not implemented')

    # This lifecycle method is called once per payload.
    def process(self, payload: Any):
        raise NotImplementedError('process not implemented')

    # Returns the arg spec of the process method.
    def processor_arg_spec(self):
        raise NotImplementedError('process not implemented')

    def num_cpus(self) -> float:
        return .5
