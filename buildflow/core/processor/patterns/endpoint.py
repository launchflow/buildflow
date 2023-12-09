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


class EndpointProcessor(ProcessorAPI):
    processor_type = ProcessorType.ENDPOINT

    def __init__(self, processor_id: ProcessorID):
        self.processor_id = processor_id

    def route_info(self) -> RouteInfo:
        raise NotImplementedError("endpoint not implemented for EndpointProcessor")

    def service_id(self) -> str:
        raise NotImplementedError("service_id not implemented for EndpointProcessor")

    # This lifecycle method is called once per payload.
    def process(self, element, **kwargs):
        raise NotImplementedError("process not implemented for EndpointProcessor")


class EndpointGroup(ProcessorGroup[EndpointProcessor]):
    group_type: ProcessorGroupType = ProcessorGroupType.SERVICE

    def __init__(
        self,
        group_id: GroupID,
        processors: List[EndpointProcessor],
        base_route: Route = "/",
        middleware: List = [],
    ):
        super().__init__(group_id, processors)
        self.base_route = base_route
        self._processor_map = {p.processor_id: p for p in processors}
        self.middleware = middleware
