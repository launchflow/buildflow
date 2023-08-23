import dataclasses

from buildflow.core.types.gcp_types import CloudSQLDatabaseName
from buildflow.io.gcp.cloud_sql_instance import CloudSQLInstance
from buildflow.io.gcp.providers.cloud_sql_database_provider import (
    CloudSQLDatabasePulumiProvider,
)
from buildflow.io.primitive import GCPPrimtive


# TODO: add generic types
@dataclasses.dataclass
class CloudSQLDatabase(GCPPrimtive[CloudSQLDatabasePulumiProvider, None, None, None]):
    database_name: CloudSQLDatabaseName
    instance: CloudSQLInstance

    def _pulumi_provider(self) -> CloudSQLDatabasePulumiProvider:
        return CloudSQLDatabasePulumiProvider(
            database_name=self.database_name, instance=self.instance
        )
