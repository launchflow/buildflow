from typing import Optional
from uuid import uuid4


def uuid(max_len: Optional[int] = None) -> str:
    if max_len is not None:
        return uuid4().hex[:max_len]
    return uuid4().hex
