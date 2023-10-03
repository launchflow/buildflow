import dataclasses
from typing import List

import pulumi
import pulumi_gcp

from buildflow.core.options.credentials_options import GCPCredentialsOptions
from buildflow.core.types.gcp_types import CloudSQLInstanceName, GCPProjectID, GCPRegion
from buildflow.io.primitive import GCPPrimtive
from buildflow.types.gcp import CloudSQLDatabaseVersion, CloudSQLInstanceSettings


# TODO: add generic types
@dataclasses.dataclass
class CloudSQLInstance(GCPPrimtive):
    instance_name: CloudSQLInstanceName
    project_id: GCPProjectID
    database_version: CloudSQLDatabaseVersion
    settings: CloudSQLInstanceSettings
    region: GCPRegion

    def primitive_id(self):
        return f"{self.project_id}:{self.instance_name}"

    def pulumi_resources(
        self, credentials: GCPCredentialsOptions, opts: pulumi.ResourceOptions
    ) -> List[pulumi.Resource]:
        return [
            pulumi_gcp.sql.DatabaseInstance(
                resource_name=self.primitive_id(),
                opts=opts,
                name=self.instance_name,
                project=self.project_id,
                database_version=self.database_version,
                settings=self.settings,
                region=self.region,
                deletion_protection=False,
            )
        ]

    def cloud_console_url(self) -> str:
        return f"https://console.cloud.google.com/sql/instances/{self.instance_name}?project={self.project_id}"
