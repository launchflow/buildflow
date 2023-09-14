import dataclasses

import pulumi

from buildflow.core.credentials.gcp_credentials import GCPCredentials
from buildflow.core.types.gcp_types import CloudSQLDatabaseName
from buildflow.io.gcp.cloud_sql_instance import CloudSQLInstance
from buildflow.io.gcp.pulumi.cloud_sql_database import CloudSQLDatabaseResource
from buildflow.io.primitive import GCPPrimtive


@dataclasses.dataclass
class CloudSQLDatabase(GCPPrimtive):
    database_name: CloudSQLDatabaseName
    instance: CloudSQLInstance

    def pulumi_resource(
        self, credentials: GCPCredentials, opts: pulumi.ResourceOptions
    ) -> CloudSQLDatabaseResource:
        return CloudSQLDatabaseResource(
            database_name=self.database_name,
            instance=self.instance,
            credentials=credentials,
            opts=opts,
        )
