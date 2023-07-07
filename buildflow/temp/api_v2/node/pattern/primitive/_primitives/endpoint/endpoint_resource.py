# ruff: noqa: E501
from buildflow.api_v2.node.pattern.primitive._primitives.endpoint.endpoint_provider import (
    EndpointProvider,
)
from buildflow.api_v2.node.pattern.primitive.resource.resource_api import ResourceAPI


class EndpointResource(ResourceAPI):
    provider: EndpointProvider
