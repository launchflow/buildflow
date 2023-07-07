# ruff: noqa: E501
from buildflow.api_v2.node.pattern.primitive._primitives.endpoint.endpoint_resource import (
    EndpointResource,
)
from buildflow.api_v2.node.pattern.primitive.primitive_api import PrimitiveAPI


class Endpoint(PrimitiveAPI):
    resource: EndpointResource
