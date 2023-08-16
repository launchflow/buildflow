import hashlib
import inspect
import json
import logging
import os
import time
from functools import wraps
from typing import Any, Dict, Optional, TypeVar
from uuid import uuid4

import yaml

from buildflow.exceptions import PathNotFoundException

# create a UUID type alias
# NOTE: python 3.8 doesn't support typing.TypeAlias
UUID: str


def write_json_file(file_path: str, data: Any):
    if not os.path.exists(os.path.dirname(file_path)):
        os.makedirs(os.path.dirname(file_path))
    json.dump(data, open(file_path, "w"), indent=4)


def read_json_file(file_path: str) -> Dict[str, Any]:
    if not os.path.exists(file_path):
        return {}
    return json.load(open(file_path, "r"))


def write_yaml_file(file_path: str, data: Any):
    if not os.path.exists(os.path.dirname(file_path)):
        os.makedirs(os.path.dirname(file_path))
    yaml.dump(data, open(file_path, "w"), indent=4, sort_keys=False)


def read_yaml_file(file_path: str) -> Dict[str, Any]:
    if not os.path.exists(file_path):
        return {}
    return yaml.safe_load(open(file_path, "r"))


def assert_path_exists(path: str):
    if not os.path.exists(path):
        raise PathNotFoundException(f"Path {path} does not exist.")


def stable_hash(obj: Any):
    """Creates a hash thats stable across runs."""
    return hashlib.sha256(str(obj).encode("utf-8")).hexdigest()


def uuid(max_len: Optional[int] = None) -> str:
    if max_len is not None:
        return str(uuid4())[:max_len]
    return str(uuid4())


def timestamp_millis() -> int:
    return int(time.monotonic() * 1000)


def get_fn_args(fn) -> inspect.FullArgSpec:
    return inspect.getfullargspec(fn)


def log_errors(endpoint: str):
    logging.debug("log_errors not implemented")

    def decorator_function(original_function):
        return original_function

    return decorator_function


T = TypeVar("T")


# This attaches a method to a class at runtime, while preserving the type signature
# of the original function.
def attach_method_to_class(cls, method_name, original_func):
    @wraps(original_func)
    async def wrapper(self, *args, **kwargs):
        if inspect.iscoroutinefunction(original_func):
            return await original_func(*args, **kwargs)
        return original_func(*args, **kwargs)

    sig = inspect.signature(original_func)
    if (
        inspect.Parameter("self", inspect.Parameter.POSITIONAL_OR_KEYWORD)
        not in sig.parameters.values()
    ):
        params = [inspect.Parameter("self", inspect.Parameter.POSITIONAL_OR_KEYWORD)]
        params.extend(sig.parameters.values())
    else:
        # If we're wrapping a class function don't add self twice
        params = sig.parameters.values()
    wrapper.__signature__ = sig.replace(parameters=params)
    setattr(cls, method_name, wrapper)


# This attaches a method to a class at runtime, while preserving the type signature
# of the original function.
def attach_wrapped_method_to_class(cls, method_name, original_func):
    @wraps(original_func)
    async def wrapper(self, *args, **kwargs):
        if inspect.iscoroutinefunction(original_func):
            return await self.instance.process(*args, **kwargs)
        else:
            return self.instance.process(*args, **kwargs)

    sig = inspect.signature(original_func)
    params = sig.parameters.values()
    wrapper.__signature__ = sig.replace(parameters=params)
    setattr(cls, method_name, wrapper)
