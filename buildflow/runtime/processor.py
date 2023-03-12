import inspect
from typing import Optional

from buildflow import utils
from buildflow.api import ProcessorAPI
from buildflow.api.io import IO, Empty
from buildflow.runtime import Runtime


class Processor(ProcessorAPI):

    def sink(self) -> IO:
        return Empty()

    def setup(self):
        pass

    def process(self, payload):
        return payload

    def processor_arg_spec(self):
        return inspect.getfullargspec(self.process)


def processor(runtime: Runtime, source: IO, sink: Optional[IO] = None):

    if sink is None:
        sink = Empty()

    def decorator_function(original_function):
        processor_id = original_function.__name__
        # Dynamically define a new class with the same structure as Processor
        class_name = f'AdHocProcessor_{utils.uuid(max_len=8)}'
        _AdHocProcessor = type(
            class_name, (object, ), {
                'source':
                lambda self: source,
                'sink':
                lambda self: sink,
                'sinks':
                lambda self: [],
                'setup':
                lambda self: None,
                'process':
                lambda self, payload: original_function(payload),
                'processor_arg_spec':
                lambda self: inspect.getfullargspec(original_function),
            })
        processor_instance = _AdHocProcessor()
        runtime.register_processor(processor_instance,
                                   processor_id=processor_id)

        def wrapper_function(*args, **kwargs):
            return original_function(*args, **kwargs)

        return wrapper_function

    return decorator_function
