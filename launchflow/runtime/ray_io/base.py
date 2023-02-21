"""Base class for all Ray IO Connectors"""

import os
from typing import Any, Callable, Dict, Iterable

from launchflow.runtime import tracer as t

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
        elements: Iterable[Dict[str, Any]],
    ):
        raise NotImplementedError(
            f'`_write` method not implemented for class {self.__class__}')

    async def write(
        self,
        elements: Iterable[Dict[str, Any]],
        context: Dict[str, str] = {},
    ):
        result = await self.remote_fn(elements)

        if self.data_tracing_enabled:
            add_to_trace(key=self.__class__.__name__,
                         value={'output_data': result},
                         context=context)

        return await self._write(result)


class RaySource:
    """Base class for all ray sources."""

    def __init__(self, ray_sinks: Dict[str, RaySink]) -> None:
        self.ray_sinks = ray_sinks
        self.data_tracing_enabled = _data_tracing_enabled()

    # NOTE: Batch runs are also modeled as a stream.
    def stream(self):
        raise NotImplementedError(
            f'`stream` method not implemented for class {self.__class__}')

    @classmethod
    def source_inputs(cls, io_ref, num_replicas: int):
        """Creates an instance of the source.

        This will be executed only once per runtime before the flow is actually
        started. It is a good place to do work that can not be serialized
        across processes. The best example of this is executing a bigquery
        query, then the subsequent actors can each read a portion of that
        query.
        """
        return [(io_ref,)] * num_replicas

    async def send_to_sinks(self, elements):
        results = {}
        for name, ray_sink in self.ray_sinks.items():
            result = await ray_sink.write.remote(elements)
            results[name] = result
        return results
