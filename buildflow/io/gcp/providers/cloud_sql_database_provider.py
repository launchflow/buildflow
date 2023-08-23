from typing import Optional, Type

import pulumi
import pulumi_gcp

from buildflow.core.credentials.gcp_credentials import GCPCredentials
from buildflow.core.types.gcp_types import CloudSQLDatabaseName
from buildflow.io.gcp.cloud_sql_instance import CloudSQLInstance
from buildflow.io.provider import PulumiProvider


class _CloudSQLDatabaseResource(pulumi.ComponentResource):
    def __init__(
        self,
        database_name: CloudSQLDatabaseName,
        instance: CloudSQLInstance,
        # pulumi_resource options (buildflow internal concept)
        type_: Optional[Type],
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

        self.instance_resource = None
        instance_pulumi_provider = instance.pulumi_provider()
        if instance_pulumi_provider is not None:
            self.instance_resource = instance_pulumi_provider.pulumi_resource(
                type_, credentials, opts
            )
            opts = pulumi.ResourceOptions.merge(
                opts, pulumi.ResourceOptions(depends_on=self.instance_resource)
            )
        self.database_resource = pulumi_gcp.sql.Database(
            resource_name=f"{instance.project_id}-{instance.instance_name}-{database_name}",
            opts=opts,
            name=database_name,
            instance=instance.instance_name,
            project=instance.project_id,
            # TODO: should probably make this configurable.
            deletion_protection=False,
        )

        outputs["gcp.sql.database"] = self.database_resource.id
        self.register_outputs(outputs)


class CloudSQLDatabasePulumiProvider(PulumiProvider):
    def __init__(
        self, *, database_name: CloudSQLDatabaseName, instance: CloudSQLInstance
    ):
        self.instance = instance
        self.database_name = database_name

    def pulumi_resource(
        self,
        type_: Optional[Type],
        credentials: GCPCredentials,
        opts: pulumi.ResourceOptions,
    ):
        return _CloudSQLDatabaseResource(
            database_name=self.database_name,
            instance=self.instance,
            type_=type_,
            credentials=credentials,
            opts=opts,
        )
