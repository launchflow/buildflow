import dataclasses
from typing import Any, Iterable

from buildflow.config.cloud_provider_config import LocalOptions
from buildflow.core.credentials.empty_credentials import EmptyCredentials
from buildflow.io.local.strategies.pulse_strategies import PulseSource
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

    def source(self, credentials: EmptyCredentials) -> PulseSource:
        return PulseSource(
            items=self.items,
            pulse_interval_seconds=self.pulse_interval_seconds,
            backlog_size=self.backlog_size,
            credentials=credentials,
        )
