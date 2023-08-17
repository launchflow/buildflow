from buildflow.core.credentials import EmptyCredentials
from buildflow.io.local.strategies.empty_strategies import EmptySink
from buildflow.io.provider import SinkProvider


class EmptyProvider(SinkProvider):
    def sink(self, credentials: EmptyCredentials):
        return EmptySink(credentials)
