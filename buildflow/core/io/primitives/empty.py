from buildflow.core.io.primitives.primitive import Primitive
from buildflow.core.options.primitive_options import PrimitiveOptions
from buildflow.core.providers.provider import (
    PulumiProvider,
    SinkProvider,
    SourceProvider,
)


class EmptyPrimitive(Primitive):
    """An empty primitive."""
    
    @classmethod
    def from_options(cls, options: PrimitiveOptions) -> "EmptyPrimitive":
        return cls()
    
    def source_provider(self) -> SourceProvider:
        raise NotImplementedError("EmptyPrimitive.source_provider() is not implemented.")
    
    def sink_provider(self) -> SinkProvider:
        raise NotImplementedError("EmptyPrimitive.sink_provider() is not implemented.")
    
    def pulumi_provider(self) -> PulumiProvider:
        raise NotImplementedError("EmptyPrimitive.pulumi_provider() is not implemented.")