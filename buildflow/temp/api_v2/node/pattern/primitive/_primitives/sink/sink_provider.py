from buildflow.api_v2.node.pattern.primitive.resource.provider.provider_api import (
    ProviderAPI,
)

from typing import Any, Callable, Type


class Batch:
    pass


class SinkProvider(ProviderAPI):
    async def push(self, batch: Batch):
        """Push pushes a batch of data to the source."""
        raise NotImplementedError("push not implemented")

    def push_converter(self, user_defined_type: Type) -> Callable[[Any], Any]:
        raise NotImplementedError("push_converter not implemented")
