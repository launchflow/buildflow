"""Base class for all Ray IO Connectors"""

import os
from typing import Any, Callable, Dict, Iterable, Union

from flowstate.runtime import tracer as t

tracer = t.RedisTracer()


def add_to_trace(
    key: str,
    value: Any,
    context: Dict[str, str],
) -> Dict[str, str]:
    """Add a key value pair to the current span.

    Args:
        key: The key to add to the span.
        value: The value to add to the span.
        context: The context state handler.

    Returns:
        The updated context.
    """
    return tracer.add_to_trace(key, value, context)


def _data_tracing_enabled() -> bool:
    return 'ENABLE_FLOW_DATA_TRACING' in os.environ


class RaySink:
    """Base class for all ray sinks."""

    def __init__(self, remote_fn: Callable) -> None:
        self.remote_fn = remote_fn
        self.data_tracing_enabled = _data_tracing_enabled()

    def _write(
        self,
        element: Union[Dict[str, Any], Iterable[Dict[str, Any]]],
    ):
        raise NotImplementedError(
            f'`_write` method not implemented for class {self.__class__}')

    async def write(
        self,
        element: Union[Dict[str, Any], Iterable[Dict[str, Any]]],
        context: Dict[str, str] = {},
    ):
        result = await self.remote_fn(element)

        if self.data_tracing_enabled:
            add_to_trace(key=self.__class__.__name__,
                         value={'output_data': result},
                         context=context)

        return self._write(result)


class RaySource:
    """Base class for all ray sources."""

    def __init__(self, ray_sinks: Iterable[RaySink]) -> None:
        self.ray_sinks = ray_sinks
        self.data_tracing_enabled = _data_tracing_enabled()

    # NOTE: Batch runs are also modeled as a stream.
    def stream(self):
        raise NotImplementedError(
            f'`stream` method not implemented for class {self.__class__}')
