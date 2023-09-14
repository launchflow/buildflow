import dataclasses
import inspect
from typing import Iterable, Optional, Tuple, Type

from buildflow.core.processor.processor import ProcessorAPI
from buildflow.dependencies.base import Dependency


@dataclasses.dataclass
class TypeWrapper:
    arg_name: str
    arg_type: Optional[Type]


def process_types(
    processor: ProcessorAPI,
) -> Tuple[Iterable[TypeWrapper], Optional[Type]]:
    """Returns the expected input type and output type of the processor."""
    full_arg_spec = inspect.getfullargspec(processor.process)
    output_type = None
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
    input_types = []
    for arg in full_arg_spec.args:
        arg_type = None
        if arg != "self":
            if arg in full_arg_spec.annotations:
                if isinstance(full_arg_spec.annotations[arg], Dependency):
                    continue
                arg_type = full_arg_spec.annotations[arg]
            input_types.append(TypeWrapper(arg_name=arg, arg_type=arg_type))
    return input_types, output_type


def add_input_types(input_types, output_type):
    def decorator(f):
        from functools import wraps
        from inspect import Parameter, signature

        @wraps(f)
        async def wrapper(*args, **kwargs):
            return await f(*args, **kwargs)

        sig = signature(f)
        params = sig.parameters
        params = list(params.values())
        new_params = []
        for input_type in input_types:
            new_param = Parameter(
                name=input_type.arg_name,
                kind=Parameter.POSITIONAL_OR_KEYWORD,
                annotation=input_type.arg_type,
            )
            new_params.append(new_param)
        final_params = [params[0]] + new_params + [params[1]]
        if output_type is None:
            new_sig = sig.replace(parameters=final_params)
        else:
            new_sig = sig.replace(
                parameters=final_params, return_annotation=output_type
            )

        wrapper.__signature__ = new_sig
        return wrapper

    return decorator
