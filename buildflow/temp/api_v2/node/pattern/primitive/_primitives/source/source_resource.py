from buildflow.api_v2.node.pattern.primitive._primitives.source.source_provider import (
    SourceProvider,
)
from buildflow.api_v2.node.pattern.primitive.resource.resource_api import ResourceAPI


class SourceResource(ResourceAPI):
    provider: SourceProvider
