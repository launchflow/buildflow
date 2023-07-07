from buildflow.api_v2.node.pattern.primitive import PrimitiveAPI
from buildflow.api_v2.node.pattern.primitive._primitives.source.source_resource import (
    SourceResource,
)


class Source(PrimitiveAPI):
    resource: SourceResource
