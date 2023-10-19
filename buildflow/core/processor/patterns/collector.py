from typing import List

from buildflow.core.processor.processor import (
    GroupID,
    ProcessorAPI,
    ProcessorGroup,
    ProcessorGroupType,
    ProcessorID,
    ProcessorType,
)
from buildflow.io.endpoint import Route, RouteInfo
from buildflow.io.strategies.sink import SinkStrategy


class CollectorProcessor(ProcessorAPI):
    processor_type = ProcessorType.COLLECTOR

    def __init__(self, processor_id: ProcessorID):
        self.processor_id = processor_id

    def route_info(self) -> RouteInfo:
        raise NotImplementedError("route_info not implemented for CollectorProcessor")

    def sink(self) -> SinkStrategy:
        raise NotImplementedError("sink not implemented for CollectorProcessor")

    # This lifecycle method is called once per payload.
    def process(self, element, **kwargs):
        raise NotImplementedError("process not implemented for CollectorProcessor")


class CollectorGroup(ProcessorGroup[CollectorProcessor]):
    group_type: ProcessorGroupType = ProcessorGroupType.COLLECTOR

    def __init__(
        self,
        group_id: GroupID,
        processors: List[CollectorProcessor],
        base_route: Route = "/",
        middleware: List = [],
    ):
        super().__init__(group_id, processors)
        self.base_route = base_route
        self._processor_map = {p.processor_id: p for p in processors}
        self.middleware = middleware
