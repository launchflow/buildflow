from typing import Any, Iterable

from buildflow.api.io import IO


class ProcessorAPI:

    # This lifecycle method defines the input reference for the processor.
    def source(self) -> IO:
        raise NotImplementedError('source not implemented')

    # This lifecycle method defines the output reference for the processor.
    def sink(self) -> IO:
        raise NotImplementedError('sink not implemented')

    # You can also define multiple outputs.
    def sinks(self) -> Iterable[IO]:
        raise NotImplementedError('sinks not implemented')

    # This lifecycle method initializes any shared state.
    def setup(self):
        raise NotImplementedError('setup not implemented')

    # This lifecycle method is called once per payload.
    def process(self, payload: Any):
        raise NotImplementedError('process not implemented')
