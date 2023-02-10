"""Base class for all Ray IO Connectors"""

from typing import Any, Dict, Iterable, Union, Optional

from flow_io import tracer as t


tracer = t.OpenTelemetryTracer()

def add_to_trace(
    key: str,
    value: Any,
    carrier: Dict[str, str],
) -> Dict[str, str]:
    """Add a key value pair to the current span.

    Args:
        key: The key to add to the span.
        value: The value to add to the span.
        carrier: The context state handler.

    Returns:
        The updated carrier.
    """
    return tracer.add_to_trace(key, value, carrier)


def _data_tracing_enabled() -> bool:
    # return 'ENABLE_FLOW_DATA_TRACING' in os.environ
    return True


class RaySource:
    """Base class for all Ray sources."""

    def __init__(self, ray_inputs: Iterable, input_node_space: str) -> None:
        self.ray_inputs = ray_inputs
        self.input_node_space = input_node_space
        self.data_tracing_enabled = _data_tracing_enabled()

    def run(self):
        raise ValueError('All Ray sources should implement: `run`')


class RaySink:
    """Base class for all Ray sinks."""

    def __init__(self) -> None:
        self.data_tracing_enabled = _data_tracing_enabled()

    def _write(
        self,
        element: Union[Dict[str, Any], Iterable[Dict[str, Any]]],
    ):
        raise ValueError('All Ray sinks should implement: `_write`')

    def write(
        self,
        element: Union[Dict[str, Any], Iterable[Dict[str, Any]]],
        carrier: Dict[str, str],
    ):
        if self.data_tracing_enabled:
            add_to_trace('output_data', element, carrier)
        return self._write(element)
