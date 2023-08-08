import dataclasses
from typing import Any, Iterable

from buildflow.config.cloud_provider_config import LocalOptions
from buildflow.io.local.providers.pulse_providers import PulseProvider
from buildflow.io.primitive import LocalPrimtive


@dataclasses.dataclass
class PulseWithBacklog(LocalPrimtive):
    items: Iterable[Any]
    pulse_interval_seconds: float
    backlog_size: int

    @classmethod
    def from_local_options(
        cls,
        local_options: LocalOptions,
        *,
        items: Iterable[Any],
        pulse_interval_seconds: float,
        backlog_size: int,
    ) -> "PulseWithBacklog":
        return cls(
            items=items,
            pulse_interval_seconds=pulse_interval_seconds,
            backlog_size=backlog_size,
        )

    def source_provider(self) -> PulseProvider:
        return PulseProvider(
            items=self.items,
            pulse_interval_seconds=self.pulse_interval_seconds,
            backlog_size=self.backlog_size,
        )

    def _pulumi_provider(self) -> PulseProvider:
        return PulseProvider(
            items=self.items,
            pulse_interval_seconds=self.pulse_interval_seconds,
            backlog_size=self.backlog_size,
        )
