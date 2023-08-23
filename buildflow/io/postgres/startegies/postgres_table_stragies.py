from buildflow.core.credentials.gcp_credentials import GCPCredentials
from buildflow.io.strategies.sink import SinkStrategy


class PostgresTableSink(SinkStrategy):
    def __init__(self, credentials: GCPCredentials):
        super().__init__(credentials, "postgres-table-sink")
