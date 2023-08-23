from typing import Optional, Type

import pulumi
import pulumi_gcp

from buildflow.core.credentials.gcp_credentials import GCPCredentials
from buildflow.core.types.gcp_types import CloudSQLInstanceName, GCPProjectID
from buildflow.io.provider import PulumiProvider
from buildflow.types.gcp import CloudSQLPostgresVersion


class _CloudSQLInstanceResource(pulumi.ComponentResource):
    def __init__(
        self,
        project_id: GCPProjectID,
        instance_name: CloudSQLInstanceName,
        database_version: CloudSQLPostgresVersion,
        # pulumi_resource options (buildflow internal concept)
        type_: Optional[Type],
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
        )

        outputs["gcp.sql.instance"] = self.instance_resource.id
        outputs[
            "buildflow.cloud_console.url"
        ] = f"https://console.cloud.google.com/sql/instances/{instance_name}?project={project_id}"

        self.register_outputs(outputs)


class CloudSQLInstancePulumiProvider(PulumiProvider):
    def __init__(
        self,
        *,
        instance_name: CloudSQLInstanceName,
        project_id: GCPProjectID,
        database_version: CloudSQLPostgresVersion,
    ):
        self.instance_name = instance_name
        self.project_id = project_id
        self.database_version = database_version

    def pulumi_resource(
        self,
        type_: Optional[Type],
        credentials: GCPCredentials,
        opts: pulumi.ResourceOptions,
    ):
        return _CloudSQLInstanceResource(
            project_id=self.project_id,
            instance_name=self.instance_name,
            database_version=self.database_version,
            type_=type_,
            credentials=credentials,
            opts=opts,
        )
