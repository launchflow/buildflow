from buildflow.api.primitives._primitive import PrimitiveAPI
from typing import Any, Callable, Type


class Batch:
    pass


class Sink(PrimitiveAPI):
    async def push(self, batch: Batch):
        """Push pushes a batch of data to the source."""
        raise NotImplementedError("push not implemented")

    def push_converter(self, user_defined_type: Type) -> Callable[[Any], Any]:
        raise NotImplementedError("push_converter not implemented")
