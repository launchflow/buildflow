from typing import Any, Callable, Type

from buildflow.core.credentials import GCPCredentials
from buildflow.core.io.utils.clients import gcp_clients
from buildflow.core.strategies.sink import Batch, SinkStrategy
from buildflow.core.types.gcp_types import GCPProjectID, GCSBucketName, GCSBucketURL


class GCSBucketSink(SinkStrategy):
    def __init__(
        self,
        *,
        credentials: GCPCredentials,
        project_id: GCPProjectID,
        bucket_name: GCSBucketName,
    ):
        super().__init__(credentials=credentials, strategy_id="gcp-gcs-bucket-sink")
        self.project_id = project_id
        self.bucket_name = bucket_name
        clients = gcp_clients.GCPClients(
            credentials=credentials,
            quota_project_id=project_id,
        )
        self.storage_client = clients.get_storage_client(project_id)

    @property
    def bucket_url(self) -> GCSBucketURL:
        raise NotImplementedError("TODO: Implement this property")

    async def push(self, batch: Batch):
        raise NotImplementedError("TODO: Implement this method")

    def push_converter(self, user_defined_type: Type) -> Callable[[Any], Any]:
        raise NotImplementedError("TODO: Implement this method")
