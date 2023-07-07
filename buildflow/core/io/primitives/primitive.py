from buildflow.core.options._options import Options
from buildflow.core.providers.provider import (
    PulumiProvider,
    SinkProvider,
    SourceProvider,
)
from buildflow.core.strategies._stategy import StategyType


PrimitiveID = str


class Primitive:
    primitive_id: PrimitiveID
    # TODO: We need to check the infra State to warn the user if the infra has not been
    # created yet.
    is_portable: bool = False

    @classmethod
    def from_options(cls, options: Options, strategy_type: StategyType) -> "Primitive":
        """Create a primitive from options."""
        raise NotImplementedError("Primitive.from_options() is not implemented.")

    def source_provider(self) -> SourceProvider:
        """Return a source provider for this primitive."""
        raise NotImplementedError("Primitive.source_provider() is not implemented.")

    def sink_provider(self) -> SinkProvider:
        """Return a sink provider for this primitive."""
        raise NotImplementedError("Primitive.sink_provider() is not implemented.")

    def pulumi_provider(self) -> PulumiProvider:
        """Return a pulumi provider for this primitive."""
        raise NotImplementedError("Primitive.pulumi_provider() is not implemented.")
