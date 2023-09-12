import dataclasses
from typing import Optional

from buildflow.core.types.gcp_types import CloudSQLInstanceName, GCPProjectID
from buildflow.io.gcp.providers.cloud_sql_instance_provider import (
    CloudSQLInstancePulumiProvider,
)
from buildflow.io.primitive import GCPPrimtive
from buildflow.types.gcp import CloudSQLPostgresVersion


# TODO: add generic types
@dataclasses.dataclass
class CloudSQLInstance(GCPPrimtive):
    instance_name: CloudSQLInstanceName
    project_id: GCPProjectID

    # Pulumi only options.
    database_version: Optional[CloudSQLPostgresVersion] = dataclasses.field(
        init=False, default=None
    )

    def options(
        self,
        database_version: CloudSQLPostgresVersion,
    ) -> "CloudSQLInstance":
        self.database_version = database_version
        return self

    def _pulumi_provider(self) -> CloudSQLInstancePulumiProvider:
        if self.database_version is None:
            raise ValueError(
                "database_version must be set for managing a cloud SQL instance."
            )
        return CloudSQLInstancePulumiProvider(
            instance_name=self.instance_name,
            project_id=self.project_id,
            database_version=self.database_version,
        )
