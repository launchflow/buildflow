import dataclasses
from typing import Iterable

from buildflow.api.processor import ProcessorAPI


class NodeResult:

    def __init__(self, node_name: str) -> None:
        self.node_name = node_name


class NodeRunResult(NodeResult):

    async def output(self, register_drain: bool = True):
        """This method will block until runtime completion."""
        pass

    async def drain(self):
        """Sends the drain signal to the running node."""
        pass

    async def stop(self):
        """Sends the stop signal to the running node."""
        pass


class NodeApplyResult(NodeResult):

    async def output(self):
        """This method will block until `node.apply(...)` completion."""
        pass


class NodeDestroyResult(NodeResult):

    async def output(self):
        """This method will block until `node.destroy(...)` completion."""
        pass


@dataclasses.dataclass
class IOPlan:
    name: str


@dataclasses.dataclass
class ProcessorPlan:
    name: str
    sources: Iterable[IOPlan]
    sinks: Iterable[IOPlan]


@dataclasses.dataclass
class NodePlan:
    name: str
    processors: Iterable[ProcessorPlan]


class NodeAPI:

    def add(self, processor: ProcessorAPI):
        raise NotImplementedError("add not implemented")

    def plan(self) -> NodePlan:
        raise NotImplementedError("plan not implemented")

    def run(self) -> NodeRunResult:
        raise NotImplementedError("run not implemented")

    def apply(self) -> NodeApplyResult:
        raise NotImplementedError("apply not implemented")

    def destroy(self) -> NodeDestroyResult:
        raise NotImplementedError("destroy not implemented")
