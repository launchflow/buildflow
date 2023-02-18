"""Base class for all Ray IO Connectors"""

import os
from typing import Any, Callable, Dict, Iterable, Union

import ray

from flow_io import tracer as t

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
    """Base class for all Ray sinks."""

    def __init__(self, remote_fn: Callable) -> None:
        self.remote_fn = remote_fn
        self.data_tracing_enabled = _data_tracing_enabled()

    def _write(
        self,
        element: Union[Dict[str, Any], Iterable[Dict[str, Any]]],
    ):
        raise ValueError('All Ray sinks should implement: `_write`')

    async def write(
        self,
        element: Union[Dict[str, Any], Iterable[Dict[str, Any]]],
        context: Dict[str, str] = {},
    ):
        # print('waiting in write')
        result = await self.remote_fn(element)
        if self.data_tracing_enabled:
            add_to_trace(self.node_space, {'output_data': result}, context)
        return self._write(result)


class RaySource:
    """Base class for all Ray sources."""

    def __init__(self, ray_sinks: Iterable[RaySink]) -> None:
        self.ray_sinks = ray_sinks
        self.data_tracing_enabled = _data_tracing_enabled()

    def run(self):
        raise ValueError('All Ray sources should implement: `run`')
