from typing import Any, Iterable

from buildflow.api.io import IO


class ProcessorAPI:

    # This static method defines the input reference for the processor.
    @staticmethod
    def source() -> IO:
        raise NotImplementedError('source not implemented')

    # This static method defines the output reference for the processor.
    @staticmethod
    def sink() -> IO:
        raise NotImplementedError('sink not implemented')

    # You can also define multiple outputs.
    @staticmethod
    def sinks() -> Iterable[IO]:
        raise NotImplementedError('sinks not implemented')

    # This lifecycle method initializes any shared state.
    def setup(self):
        raise NotImplementedError('setup not implemented')

    # This lifecycle method is called once per payload.
    def process(self, payload: Any):
        raise NotImplementedError('process not implemented')
