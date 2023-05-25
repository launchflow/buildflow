import dataclasses
from typing import Any, Iterable, Optional

from buildflow.api.processor import ProcessorAPI, ProcessorPlan


class NodeResults:
    def __init__(self, node_name: str) -> None:
        self.node_name = node_name

    async def output(self, register_shutdown: bool = True):
        """This method will block the flow until completion."""
        pass

    async def shutdown(self):
        """Sends the shutdown signal to the running flow."""
        pass


@dataclasses.dataclass
class NodePlan:
    name: Optional[str]
    processors: Iterable[ProcessorPlan]


class NodeAPI:
    def __init__(self, name) -> None:
        self.name = name

    def processor(input, output: Optional[Any] = None):
        pass

    def add_processor(self, processor: ProcessorAPI):
        pass

    def run(
        *,
        disable_usage_stats: bool = False,
        disable_resource_creation: bool = True,
        blocking: bool = True,
    ):
        pass

    def plan(self) -> NodePlan:
        pass

    def setup():
        pass
