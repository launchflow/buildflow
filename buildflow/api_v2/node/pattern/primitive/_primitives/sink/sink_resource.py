from buildflow.api_v2.node.pattern.primitive._primitives.sink.sink_provider import (
    SinkProvider,
)
from buildflow.api_v2.node.pattern.primitive.resource.resource_api import ResourceAPI


class SinkResource(ResourceAPI):
    provider: SinkProvider
