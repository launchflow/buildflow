import dataclasses

from buildflow.config.cloud_provider_config import LocalOptions
from buildflow.io.local.providers.empty_provider import EmptyProvider
from buildflow.io.primitive import LocalPrimtive


@dataclasses.dataclass
class Empty(
    LocalPrimtive[
        # Pulumi provider type
        None,
        # Source provider type
        None,
        # Sink provider type
        EmptyProvider,
        # Background task provider type
        None,
    ]
):
    @classmethod
    def from_local_options(
        cls,
        local_options: LocalOptions,
    ) -> "Empty":
        return cls()

    def sink_provider(self) -> EmptyProvider:
        return EmptyProvider()
