# ruff: noqa: E501
from buildflow.api_v2.node.pattern import PatternAPI
from buildflow.api_v2.node.pattern.primitive._primitives.endpoint import (
    Endpoint,
    Request,
    Response,
)

ServiceID = str


class Service(PatternAPI):
    service_id: ServiceID
    endpoint: Endpoint

    # This lifecycle method is called once per payload.
    def process(self, request: Request, **kwargs) -> Response:
        raise NotImplementedError("process not implemented")
