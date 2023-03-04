from typing import Any, Iterable

from buildflow.api.io import IO


class ProcessorAPI:

    # This static method defines the input reference for the processor.
    @staticmethod
    def _input() -> IO:
        raise NotImplementedError('_input not implemented')

    # This static method defines the output reference for the processor.
    @staticmethod
    def _output() -> IO:
        raise NotImplementedError('_output not implemented')

    # You can also define multiple outputs.
    @staticmethod
    def _outputs() -> Iterable[IO]:
        raise NotImplementedError('_outputs not implemented')

    # This lifecycle method initializes any shared state.
    def _setup(self):
        raise NotImplementedError('_setup not implemented')

    # This lifecycle method is called once per payload.
    def process(self, payload: Any):
        raise NotImplementedError('process not implemented')
