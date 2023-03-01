import uuid
from typing import Optional

from buildflow.api import ProcessorAPI
from buildflow.api.resources import IO, Empty
from buildflow.runtime.runner import Runtime


class Processor(ProcessorAPI):

    @staticmethod
    def _output() -> IO:
        return Empty()

    def _setup(self):
        pass

    def process(self, payload):
        return payload


def processor(input_ref: IO, output_ref: Optional[IO] = None):
    runtime = Runtime()

    if output_ref is None:
        output_ref = Empty()

    def decorator_function(original_function):
        processor_id = original_function.__qualname__
        # Dynamically define a new ProcessorAPI subclass
        class_name = f'AdHocProcessor_{uuid.uuid4().hex[:8]}'
        _AdHocProcessor = type(
            class_name, (ProcessorAPI, ), {
                '_input': staticmethod(lambda: input_ref),
                '_output': staticmethod(lambda: output_ref),
                '_outputs': staticmethod(lambda: []),
                '_setup': lambda self: None,
                'process': lambda self, payload: original_function(payload),
                'process_async': lambda self, payload: payload,
            })
        runtime.register_processor(_AdHocProcessor,
                                   input_ref,
                                   output_ref,
                                   processor_id=processor_id)

        def wrapper_function(*args, **kwargs):
            return original_function(*args, **kwargs)

        return wrapper_function

    return decorator_function
