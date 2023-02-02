"""Base class for all Ray IO Connectors"""

from typing import Any, Dict, Iterable


class RaySource:
    """Base class for all Ray sources."""

    def __init__(self, ray_inputs: Iterable, input_node_space: str) -> None:
        self.ray_inputs = ray_inputs
        self.input_node_space = input_node_space

    def run(self):
        raise ValueError('All Ray sources should implement: `run`')


class RaySink:
    """Base class for all Ray sinks."""

    async def write(self, element: Dict[str, Any]):
        raise ValueError('All Ray sinks should implement: `write`')
