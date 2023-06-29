from buildflow.api_v2.node.pattern.primitive._primitives.sink.sink_resource import (
    SinkResource,
)
from buildflow.api_v2.node.pattern.primitive.primitive_api import PrimitiveAPI


class Sink(PrimitiveAPI):
    resource: SinkResource
