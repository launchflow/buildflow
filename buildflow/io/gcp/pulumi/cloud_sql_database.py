import pulumi
import pulumi_gcp

from buildflow.core.credentials.gcp_credentials import GCPCredentials
from buildflow.core.types.gcp_types import CloudSQLDatabaseName
from buildflow.io.gcp.cloud_sql_instance import CloudSQLInstance


class CloudSQLDatabaseResource(pulumi.ComponentResource):
    def __init__(
        self,
        database_name: CloudSQLDatabaseName,
        instance: CloudSQLInstance,
        # pulumi_resource options (buildflow internal concept)
        credentials: GCPCredentials,
        opts: pulumi.ResourceOptions,
    ):
        super().__init__(
            "buildflow:gcp:sql:CloudSQLDatabase",
            f"buildflow-{instance.project_id}-{instance.instance_name}-{database_name}",
            None,
            opts,
        )
        opts = pulumi.ResourceOptions.merge(opts, pulumi.ResourceOptions(parent=self))

        outputs = {}

        self.database_resource = pulumi_gcp.sql.Database(
            resource_name=f"{instance.project_id}-{instance.instance_name}-{database_name}",
            opts=opts,
            name=database_name,
            instance=instance.instance_name,
            project=instance.project_id,
        )

        outputs["gcp.sql.database"] = self.database_resource.id
        self.register_outputs(outputs)
