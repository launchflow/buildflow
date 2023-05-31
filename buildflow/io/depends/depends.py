from typing import Any, TypeVar, Generic

from buildflow.io.providers import PushProvider

T = TypeVar("T")


class UnsupportDepenendsSource(Exception):
    def __init__(self, source: Any):
        super().__init__(
            f"Depends is not supported for sources of type: {type(source)}"
        )


class Push(Generic[T]):
    def __init__(self, push_provider: PushProvider) -> None:
        self.push_provider = push_provider

    async def push(self, element: T):
        await self.push_provider.push(element)


def Depends(depends):
    if hasattr(depends, "provider"):
        if callable(depends.provider) and isinstance(depends.provider(), PushProvider):
            return Push(depends.provider())
    raise UnsupportDepenendsSource(depends)
