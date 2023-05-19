import dataclasses
import datetime
from typing import Any, Dict, Optional
from uuid import uuid4


def uuid(max_len: Optional[int] = None) -> str:
    if max_len is not None:
        return uuid4().hex[:max_len]
    return uuid4().hex


def dataclass_to_json(dataclass_instance) -> Dict[str, Any]:
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
        if dataclasses.is_dataclass(val):
            val = dataclass_to_json(val)
        if (isinstance(val, list) and len(val) > 0
                and dataclasses.is_dataclass(val[0])):
            val = [dataclass_to_json(v) for v in val]
        to_ret[k] = val
    return to_ret
