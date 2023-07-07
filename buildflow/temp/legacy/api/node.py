import dataclasses
from typing import Iterable

from buildflow.api.processor import ProcessorAPI


class AsyncResult:
    def run_until_complete(self) -> bool:
        """This method will block until the async task is complete."""
        raise NotImplementedError("run_until_complete not implemented")


class NodeRunResult(AsyncResult):
    def drain(self, block: bool) -> bool:
        """Sends the drain signal to the running node."""
        raise NotImplementedError("drain not implemented")


class NodeApplyResult(AsyncResult):
    def stop(self):
        """Sends the stop signal to the async task."""
        raise NotImplementedError("stop not implemented")


class NodeDestroyResult(AsyncResult):
    def stop(self):
        """Sends the stop signal to the async task."""
        raise NotImplementedError("stop not implemented")


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


NodeID = str


class NodeAPI:
    node_id: NodeID

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
