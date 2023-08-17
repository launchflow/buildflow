import inspect
from typing import Optional, Tuple, Type

from buildflow.core.processor.processor import ProcessorAPI


def process_types(processor: ProcessorAPI) -> Tuple[Optional[Type], Optional[Type]]:
    """Returns the expected input type and output type of the processor."""
    full_arg_spec = inspect.getfullargspec(processor.process)
    output_type = None
    input_type = None
    if "return" in full_arg_spec.annotations:
        output_type = full_arg_spec.annotations["return"]
        if (
            hasattr(output_type, "__origin__")
            and (output_type.__origin__ is list or output_type.__origin__ is tuple)
            and hasattr(output_type, "__args__")
        ):
            # We will flatten the return type if the outter most type is a tuple or
            # list.
            output_type = output_type.__args__[0]
    if (
        len(full_arg_spec.args) > 1
        and full_arg_spec.args[1] in full_arg_spec.annotations
    ):
        input_type = full_arg_spec.annotations[full_arg_spec.args[1]]
    return input_type, output_type
