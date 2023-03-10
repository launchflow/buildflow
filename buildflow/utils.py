from typing import Any, Dict, Optional
from uuid import uuid4


def uuid(max_len: Optional[int] = None) -> str:
    if max_len is not None:
        return uuid4().hex[:max_len]
    return uuid4().hex


def asdict(dataclass_instance) -> Dict[str, Any]:
    # NOTE: we roll our own asdict instead of using dataclasses.asdict because
    # of an issue with dataclasses and cloudpickle.
    # https://github.com/cloudpipe/cloudpickle/issues/386
    return {
        k: getattr(dataclass_instance, k)
        for k in dataclass_instance.__dataclass_fields__
    }
