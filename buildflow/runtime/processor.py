import uuid
from typing import Optional

from buildflow.api.resources import IO, Empty
from buildflow.runtime import Runtime
from buildflow.api import ProcessorAPI


class Processor(ProcessorAPI):

    @staticmethod
    def _output() -> IO:
        return Empty()

    def _setup(self):
        pass

    def process(self, payload):
        return payload


def processor(runtime: Runtime,
              input_ref: IO,
              output_ref: Optional[IO] = None):

    if output_ref is None:
        output_ref = Empty()

    def decorator_function(original_function):
        processor_id = original_function.__name__
        # Dynamically define a new class with the same structure as Processor
        class_name = f'AdHocProcessor_{uuid.uuid4().hex[:8]}'
        _AdHocProcessor = type(
            class_name, (object, ), {
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
