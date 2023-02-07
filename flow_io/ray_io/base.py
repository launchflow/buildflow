"""Base class for all Ray IO Connectors"""

import os
from typing import Any, Dict, Iterable, Union


from opentelemetry import trace


def _data_tracing_enabled() -> bool:
    return 'ENABLE_FLOW_DATA_TRACING' in os.environ


class RaySource:
    """Base class for all Ray sources."""

    def __init__(self, ray_inputs: Iterable, input_node_space: str) -> None:
        self.ray_inputs = ray_inputs
        self.input_node_space = input_node_space
        self.data_tracing_enabled = _data_tracing_enabled()

    def _run(self):
        raise ValueError('All Ray sources should implement: `_`')

    def run(self):
        return self._run()


class RaySink:
    """Base class for all Ray sinks."""

    def __init__(self) -> None:
        self.data_tracing_enabled = _data_tracing_enabled()

    async def _write(
        self,
        element: Union[Dict[str, Any], Iterable[Dict[str, Any]]],
    ):
        raise ValueError('All Ray sinks should implement: `_write`')

    async def write(
        self,
        element: Union[Dict[str, Any], Iterable[Dict[str, Any]]],
    ):
        if self.data_tracing_enabled:
            current_span = trace.get_current_span()
            current_span.set_attribute('output_data', element)
        return self._write(element)
