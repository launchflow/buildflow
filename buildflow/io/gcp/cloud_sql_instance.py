import dataclasses

from buildflow.core.types.gcp_types import CloudSQLInstanceName, GCPProjectID
from buildflow.io.gcp.providers.cloud_sql_instance_provider import (
    CloudSQLInstancePulumiProvider,
)
from buildflow.io.primitive import GCPPrimtive
from buildflow.types.gcp import CloudSQLPostgresVersion

_DEFAULT_POSTGRES_VERSION = CloudSQLPostgresVersion.POSTGRES_15


# TODO: add generic types
@dataclasses.dataclass
class CloudSQLInstance(GCPPrimtive):
    instance_name: CloudSQLInstanceName
    project_id: GCPProjectID

    # Pulumi only options.
    database_version: CloudSQLPostgresVersion = dataclasses.field(
        init=False, default=_DEFAULT_POSTGRES_VERSION
    )

    def options(
        self,
        managed: bool = False,
        database_version: CloudSQLPostgresVersion = _DEFAULT_POSTGRES_VERSION,
    ) -> "CloudSQLInstance":
        to_ret = super().options(managed)
        to_ret.database_version = database_version
        return to_ret

    def _pulumi_provider(self) -> CloudSQLInstancePulumiProvider:
        return CloudSQLInstancePulumiProvider(
            instance_name=self.instance_name,
            project_id=self.project_id,
            database_version=self.database_version,
        )
