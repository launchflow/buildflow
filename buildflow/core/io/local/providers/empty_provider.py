from buildflow.core.credentials import EmptyCredentials
from buildflow.core.io.local.strategies.empty_strategies import EmptySink
from buildflow.core.providers.provider import SinkProvider


class EmptryProvider(SinkProvider):
    def sink(self, credentials: EmptyCredentials):
        return EmptySink(credentials)
