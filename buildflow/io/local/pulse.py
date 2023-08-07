import dataclasses
from typing import Any, Iterable

from buildflow.config.cloud_provider_config import LocalOptions
from buildflow.core.io.local.providers.pulse_providers import PulseProvider
from buildflow.core.io.primitive import LocalPrimtive


@dataclasses.dataclass
class Pulse(LocalPrimtive):
    items: Iterable[Any]
    pulse_interval_seconds: float

    @classmethod
    def from_local_options(
        cls,
        local_options: LocalOptions,
        *,
        items: Iterable[Any],
        pulse_interval_seconds: float,
    ) -> "Pulse":
        return cls(
            items=items,
            pulse_interval_seconds=pulse_interval_seconds,
        )

    def source_provider(self) -> PulseProvider:
        return PulseProvider(
            items=self.items,
            pulse_interval_seconds=self.pulse_interval_seconds,
        )

    def _pulumi_provider(self) -> PulseProvider:
        return PulseProvider(
            items=self.items,
            pulse_interval_seconds=self.pulse_interval_seconds,
        )
