import uuid
from typing import Optional

from flowstate.api.resources import IO, Empty
from flowstate.runtime import Runtime


def processor(input_ref: IO, output_ref: Optional[IO] = None):
    runtime = Runtime()

    if output_ref is None:
        output_ref = Empty()

    def decorator_function(original_function):
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
        runtime.register_processor(_AdHocProcessor, input_ref, output_ref)

        def wrapper_function(*args, **kwargs):
            return original_function(*args, **kwargs)

        return wrapper_function

    return decorator_function
