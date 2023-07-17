from typing import Any, Callable, Type

from buildflow.core.io.utils.schemas import converters
from buildflow.core.strategies.sink import SinkStrategy


class EmptySink(SinkStrategy):
    def __init__(self):
        super().__init__(strategy_id="local-empty-sink")

    def push_converter(self, user_defined_type: Type) -> Callable[[Any], Any]:
        return converters.identity()

    async def push(self, elements: Any):
        # Just drop the elements on the floor
        pass
