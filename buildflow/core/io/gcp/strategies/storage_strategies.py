from typing import Any, Callable, Type

from buildflow.core.options.runtime_options import RuntimeOptions
from buildflow.core.io.utils.clients import gcp_clients
from buildflow.core.strategies.sink import Batch, SinkStrategy
from buildflow.core.types.gcp_types import GCPProjectID, GCSBucketName, GCSBucketURL


class GCSBucketSink(SinkStrategy):
    def __init__(
        self,
        *,
        runtime_options: RuntimeOptions,
        project_id: GCPProjectID,
        bucket_name: GCSBucketName,
    ):
        super().__init__(
            runtime_options=runtime_options, strategy_id="gcp-gcs-bucket-sink"
        )
        self.project_id = project_id
        self.bucket_name = bucket_name
        clients = gcp_clients.GCPClients(
            gcp_credentials_file=runtime_options.gcp_credentials_file,
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
