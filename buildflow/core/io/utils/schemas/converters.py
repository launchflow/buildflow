"""Converters user by push/pull converter methods.

TODO: I think we should make this more standard instead of having a bunch
of methods. Maybe (push|pull)_converter returns a specific type that has to
have the given methods impelemeted? The the pull process push actor can
just use the __call__ method to convert

Cause there's really a couple cases every converter needs to provide:
1. not type = return identity
2. Flatten type = return converter for inner type
3. User provided conversion functions (from_bytes, to_bytes, etc..)
4. Type is expected type (e.g. bytes -> bytes) return identity
5. We know how to convert the type (e.g. dataclass -> json)
"""

import datetime
import json
from dataclasses import is_dataclass
from typing import Any, Callable, Dict, Optional, Type

import dacite
import pandas as pd

from buildflow import exceptions


def str_to_datetime(s: str) -> datetime.datetime:
    return pd.Timestamp(s).to_pydatetime()


def identity():
    return lambda x: x


def bytes_to_dict() -> Callable[[bytes], Dict[str, Any]]:
    return lambda bytes_: json.loads(bytes_.decode())


def str_to_dict() -> Callable[[str], Dict[str, Any]]:
    return lambda str_: json.loads(str_)


def bytes_to_dataclass(type_: Type) -> Callable[[bytes], Any]:
    return lambda bytes_: dacite.from_dict(
        type_,
        json.loads(bytes_.decode()),
        config=dacite.Config(type_hooks={datetime.datetime: str_to_datetime}),
    )


def str_to_dataclass(type_: Type) -> Callable[[str], Any]:
    return lambda s: dacite.from_dict(
        type_,
        json.loads(s),
        config=dacite.Config(type_hooks={datetime.datetime: str_to_datetime}),
    )


def _dataclass_to_json(dataclass_instance) -> Dict[str, Any]:
    # NOTE: we roll our own asdict instead of using dataclasses.asdict because
    # of an issue with dataclasses and cloudpickle.
    # https://github.com/cloudpipe/cloudpickle/issues/386
    # This also converts some field types that we know aren't serializable to
    # json.
    #   - datetime.datetime, datetime.date, datetime.time

    # TODO: need to ensure we convert containers of dataclasses to json
    to_ret = {}
    for k in dataclass_instance.__dataclass_fields__:
        val = getattr(dataclass_instance, k)
        if isinstance(val, (datetime.datetime, datetime.date, datetime.time)):
            val = val.isoformat()
        if is_dataclass(val):
            val = _dataclass_to_json(val)
        if isinstance(val, list) and len(val) > 0 and is_dataclass(val[0]):
            val = [_dataclass_to_json(v) for v in val]
        to_ret[k] = val
    return to_ret


def dataclass_to_json() -> Callable[[Any], Dict[str, Any]]:
    return lambda user_type: _dataclass_to_json(user_type)


def dataclass_to_bytes() -> Callable[[Any], bytes]:
    return lambda user_type: json.dumps(_dataclass_to_json(user_type)).encode("utf-8")


def json_push_converter(type_: Optional[Type]) -> Callable[[Any], Dict[str, Any]]:
    if type_ is None:
        return identity()

    origin = type_
    if hasattr(type_, "__origin__"):
        origin = type_.__origin__
    if hasattr(type_, "to_json"):
        return lambda output: type_.to_json(output)
    if is_dataclass(type_):
        return dataclass_to_json()
    if origin is list or origin is set or origin is tuple:
        if not hasattr(type_, "__args__"):
            return identity()
        converter = json_push_converter(type_.__args__[0])
        return lambda output: origin(converter(v) for v in output)
    if origin is dict:
        return identity()
    raise exceptions.CannotConvertSinkException(
        "Cannot convert from type to dictionary: `{type_}`"
    )


def bytes_push_converter(type_: Optional[Type]) -> Callable[[Any], bytes]:
    if type_ is None:
        return identity()

    origin = type_
    if hasattr(type_, "__origin__"):
        origin = type_.__origin__
    # have to special case int here since there is a builtin to_bytes method
    if hasattr(type_, "to_bytes") and type_ is not int:
        return lambda output: type_.to_bytes(output)
    if origin is bytes:
        return identity()
    else:
        # Try to serialize it to json then encode it.
        try:
            json_converter = json_push_converter(type_)
            return lambda output: json.dumps(json_converter(output)).encode("utf-8")
        except exceptions.CannotConvertSinkException:
            raise exceptions.CannotConvertSinkException(
                "Cannot convert from type to bytes: `{type_}`"
            )


def str_push_converter(type_: Optional[Type]) -> Callable[[Any], str]:
    if type_ is None:
        return identity()

    origin = type_
    if hasattr(type_, "__origin__"):
        origin = type_.__origin__
    if hasattr(type_, "to_string"):
        return lambda output: type_.to_string(output)
    if origin is str:
        return identity()
    else:
        # Try to serialize it to json then encode it.
        try:
            json_converter = json_push_converter(type_)
            return lambda output: json.dumps(json_converter(output))
        except exceptions.CannotConvertSinkException:
            raise exceptions.CannotConvertSinkException(
                "Cannot convert from type to bytes: `{type_}`"
            )


def str_pull_converter(type_: Optional[Type]) -> Callable[[str], Any]:
    if type_ is None:
        return identity()
    elif hasattr(type_, "from_string"):
        return lambda output: type_.from_string(output)
    elif is_dataclass(type_):
        return str_to_dataclass(type_)
    else:
        if hasattr(type_, "__origin__"):
            type_ = type_.__origin__
        if issubclass(type_, str):
            return identity()
        elif issubclass(type_, dict):
            return str_to_dict()
        else:
            raise exceptions.CannotConvertSourceException(
                f"Cannot convert from str to type: `{type_}`"
            )
