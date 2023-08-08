from typing import Any, Callable, Type

from buildflow.core.credentials import EmptyCredentials
from buildflow.io.strategies.sink import SinkStrategy
from buildflow.io.utils.schemas import converters


class EmptySink(SinkStrategy):
    def __init__(self, credentials: EmptyCredentials):
        super().__init__(credentials, "local-empty-sink")

    def push_converter(self, user_defined_type: Type) -> Callable[[Any], Any]:
        return converters.identity()

    async def push(self, elements: Any):
        # Just drop the elements on the floor
        pass
