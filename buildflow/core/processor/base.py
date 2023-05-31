from buildflow.api import ProcessorAPI, SinkType
from buildflow.io.registry import EmptySink


class Processor(ProcessorAPI):

    def __init__(self, name: str = '') -> None:
        self.name = name

    @classmethod
    def sink(self) -> SinkType:
        return EmptySink()

    def setup(self):
        pass

    def _process(self, payload):
        return self.process(self.source().preprocess(payload))

    def process(self, payload):
        return payload
