import inspect
from typing import Optional, Type

from buildflow.api import ProcessorAPI, SinkType
from buildflow.io.registry import EmptySink


class Processor(ProcessorAPI):
    def __init__(self, name: str = "") -> None:
        self.name = name

    @classmethod
    def sink(self) -> SinkType:
        return EmptySink()

    def setup(self):
        pass

    def process(self, payload):
        return payload

    def input_type(self) -> Optional[Type]:
        full_arg_spec = inspect.getfullargspec(self.process)
        if "return" in full_arg_spec.annotations:
            return full_arg_spec.annotations["return"]
        return None

    def output_type(self) -> Optional[Type]:
        full_arg_spec = inspect.getfullargspec(self.process)
        if (
            len(full_arg_spec.args) > 1
            and full_arg_spec.args[1] in full_arg_spec.annotations
        ):
            return full_arg_spec.annotations[full_arg_spec.args[0]]
        return None
