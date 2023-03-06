from typing import Optional

from buildflow import utils
from buildflow.api import ProcessorAPI
from buildflow.api.io import IO, Empty
from buildflow.runtime import Runtime


class Processor(ProcessorAPI):

    @staticmethod
    def sink() -> IO:
        return Empty()

    def setup(self):
        pass

    def process(self, payload):
        return payload


def processor(runtime: Runtime, source: IO, sink: Optional[IO] = None):

    if sink is None:
        sink = Empty()

    def decorator_function(original_function):
        processor_id = original_function.__name__
        # Dynamically define a new class with the same structure as Processor
        class_name = f'AdHocProcessor_{utils.uuid(max_len=8)}'
        _AdHocProcessor = type(
            class_name, (object, ), {
                'source': staticmethod(lambda: source),
                'sink': staticmethod(lambda: sink),
                'sinks': staticmethod(lambda: []),
                'setup': lambda self: None,
                'process': lambda self, payload: original_function(payload),
            })
        runtime.register_processor(_AdHocProcessor,
                                   source,
                                   sink,
                                   processor_id=processor_id)

        def wrapper_function(*args, **kwargs):
            return original_function(*args, **kwargs)

        return wrapper_function

    return decorator_function
