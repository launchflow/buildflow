from typing import Any, Iterable

from buildflow.core.credentials import EmptyCredentials
from buildflow.io.local.strategies.pulse_strategies import PulseSource
from buildflow.io.provider import SourceProvider


class PulseProvider(SourceProvider):
    def __init__(
        self,
        *,
        items: Iterable[Any],
        pulse_interval_seconds: float,
        backlog_size: int = 0,
        # source-only options
        # sink-only options
        # pulumi-only options
    ):
        self.items = items
        self.pulse_interval_seconds = pulse_interval_seconds
        self.backlog_size = backlog_size
        # sink-only options
        # pulumi-only options

    def source(self, credentials: EmptyCredentials):
        return PulseSource(
            credentials=credentials,
            items=self.items,
            pulse_interval_seconds=self.pulse_interval_seconds,
            backlog_size=self.backlog_size,
        )
