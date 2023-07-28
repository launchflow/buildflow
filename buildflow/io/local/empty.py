import dataclasses

from buildflow.config.cloud_provider_config import LocalOptions
from buildflow.core.io.local.providers.empty_provider import EmptryProvider
from buildflow.core.io.primitive import LocalPrimtive


@dataclasses.dataclass
class Empty(LocalPrimtive):
    @classmethod
    def from_local_options(
        cls,
        local_options: LocalOptions,
    ) -> "Empty":
        return cls()

    def sink_provider(self) -> EmptryProvider:
        return EmptryProvider()
