from buildflow.core.io.primitives.primitive import Primitive
import dataclasses
from buildflow.core.options.primitive_options import PrimitiveOptions, CloudProvider
from buildflow.core.io.primitives.gcp.pubsub import (
    GCPPubSubTopic,
    GCPPubSubSubscription,
)
from buildflow.core.strategies._stategy import StategyType


@dataclasses.dataclass
class Topic(Primitive):
    is_portable = True

    @classmethod
    def from_options(
        cls, options: PrimitiveOptions, strategy_type: StategyType
    ) -> Primitive:
        # GCP Implementations
        if options.cloud_provider == CloudProvider.GCP:
            if strategy_type == StategyType.SOURCE:
                return GCPPubSubSubscription.from_options(options=options.gcp)
            elif strategy_type == StategyType.SINK:
                return GCPPubSubTopic.from_options(options=options.gcp)
            else:
                raise ValueError(
                    f"Unsupported strategy type for Topic (GCP): {strategy_type}"
                )
        # AWS Implementations
        elif options.cloud_provider == CloudProvider.AWS:
            raise NotImplementedError("AWS is not implemented for Topic.")
        # Azure Implementations
        elif options.cloud_provider == CloudProvider.AZURE:
            raise NotImplementedError("Azure is not implemented for Topic.")
        # Local Implementations
        elif options.cloud_provider == CloudProvider.LOCAL:
            raise NotImplementedError("Local is not implemented for Topic.")
        # Sanity check
        else:
            raise ValueError(f"Unknown resource provider: {options.cloud_provider}")
