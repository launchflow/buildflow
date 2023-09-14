import dataclasses

import pulumi

from buildflow.core.options.credentials_options import GCPCredentialsOptions
from buildflow.core.types.gcp_types import CloudSQLInstanceName, GCPProjectID, GCPRegion
from buildflow.io.gcp.pulumi.cloud_sql_instance import CloudSQLInstanceResource
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

    def pulumi_resource(
        self, credentials: GCPCredentialsOptions, opts: pulumi.ResourceOptions
    ) -> CloudSQLInstanceResource:
        return CloudSQLInstanceResource(
            project_id=self.project_id,
            instance_name=self.instance_name,
            database_version=self.database_version,
            settings=self.settings,
            region=self.region,
            credentials=credentials,
            opts=opts,
        )
