import dataclasses
from typing import List

import pulumi
import pulumi_gcp

from buildflow.core.credentials.gcp_credentials import GCPCredentials
from buildflow.core.types.gcp_types import CloudSQLDatabaseName
from buildflow.io.gcp.cloud_sql_instance import CloudSQLInstance
from buildflow.io.primitive import GCPPrimtive


@dataclasses.dataclass
class CloudSQLDatabase(GCPPrimtive):
    database_name: CloudSQLDatabaseName
    instance: CloudSQLInstance

    def primitive_id(self):
        return (
            f"{self.instance.project_id}:{self.instance.instance_name}"
            f".{self.database_name}"
        )

    def pulumi_resources(
        self, credentials: GCPCredentials, opts: pulumi.ResourceOptions
    ) -> List[pulumi.Resource]:
        return [
            pulumi_gcp.sql.Database(
                resource_name=self.primitive_id(),
                opts=opts,
                name=self.database_name,
                instance=self.instance.instance_name,
                project=self.instance.project_id,
            )
        ]
