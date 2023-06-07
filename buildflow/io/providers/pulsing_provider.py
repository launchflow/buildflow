import asyncio
from typing import Any, Callable, Iterable, Type

from buildflow.io.providers import PullProvider
from buildflow.io.providers.base import AckInfo, PullResponse
from buildflow.io.providers.schemas import converters


class PulsingProvider(PullProvider):
    def __init__(
        self,
        items: Iterable[Any],
        pulse_interval_seconds: float,
    ):
        """
        Creates a PulsingProvider that will emit the given items at the given interval.

        Args:
            items: The items to emit.
            pulse_interval_seconds: The interval between each item being emitted.
            should_repeat: Whether to repeat the items after they have all been emitted.
                Defaults to True.
        """
        super().__init__()
        self.items = items
        self.pulse_interval_seconds = pulse_interval_seconds
        self._to_emit = 0

    async def pull(self) -> PullResponse:
        await asyncio.sleep(self.pulse_interval_seconds)
        item = self.items[self._to_emit]
        self._to_emit += 1
        if self._to_emit == len(self.items):
            self._to_emit = 0
        return PullResponse([item], None)

    def pull_converter(self, user_defined_type: Type) -> Callable[[Any], Any]:
        return converters.identity()

    async def ack(self, to_ack: AckInfo, success: bool):
        pass

    def backlog(self) -> int:
        return 0
