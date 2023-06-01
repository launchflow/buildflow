import datetime
from dataclasses import is_dataclass
import json
from typing import Any, Dict, Callable, Type


def identity():
    return lambda x: x


def bytes_to_dataclass(type_: Type) -> Callable[[bytes], Any]:
    return lambda bytes_: type_(**json.loads(bytes_.decode()))


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
    return lambda user_type: json.dumps(_dataclass_to_json(user_type)).encode()
