import dataclasses
from typing import Any, Iterable

from buildflow.config.cloud_provider_config import LocalOptions
from buildflow.core.credentials.empty_credentials import EmptyCredentials
from buildflow.core.utils import uuid
from buildflow.io.local.strategies.pulse_strategies import PulseSource
from buildflow.io.primitive import LocalPrimtive


@dataclasses.dataclass
class Pulse(LocalPrimtive):
    items: Iterable[Any]
    pulse_interval_seconds: float

    def __post_init__(self):
        self._primitive_id = uuid()

    def primitive_id(self):
        return self._primitive_id

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

    def source(self, credentials: EmptyCredentials) -> PulseSource:
        return PulseSource(
            items=self.items,
            pulse_interval_seconds=self.pulse_interval_seconds,
            credentials=credentials,
        )
