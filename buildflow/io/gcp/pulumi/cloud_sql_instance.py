import pulumi
import pulumi_gcp

from buildflow.core.credentials.gcp_credentials import GCPCredentials
from buildflow.core.types.gcp_types import CloudSQLInstanceName, GCPProjectID, GCPRegion
from buildflow.types.gcp import CloudSQLDatabaseVersion, CloudSQLInstanceSettings


class CloudSQLInstanceResource(pulumi.ComponentResource):
    def __init__(
        self,
        project_id: GCPProjectID,
        instance_name: CloudSQLInstanceName,
        database_version: CloudSQLDatabaseVersion,
        settings: CloudSQLInstanceSettings,
        region: GCPRegion,
        # pulumi_resource options (buildflow internal concept)
        credentials: GCPCredentials,
        opts: pulumi.ResourceOptions,
    ):
        super().__init__(
            "buildflow:gcp:sql:CloudSQLInstance",
            f"buildflow-{project_id}-{instance_name}",
            None,
            opts,
        )

        outputs = {}

        self.instance_resource = pulumi_gcp.sql.DatabaseInstance(
            resource_name=f"{project_id}-{instance_name}",
            opts=pulumi.ResourceOptions(parent=self),
            name=instance_name,
            project=project_id,
            database_version=database_version,
            settings=settings,
            region=region,
        )

        outputs["gcp.sql.instance"] = self.instance_resource.id
        outputs[
            "buildflow.cloud_console.url"
        ] = f"https://console.cloud.google.com/sql/instances/{instance_name}?project={project_id}"

        self.register_outputs(outputs)
