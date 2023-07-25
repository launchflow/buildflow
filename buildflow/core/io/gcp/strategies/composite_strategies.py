import dataclasses
from typing import Any, Callable, Optional, Type

from google.cloud import storage

from buildflow.core.io.utils.clients import gcp_clients
from buildflow.core.credentials import GCPCredentials
from buildflow.core.io.gcp.strategies.pubsub_strategies import (
    GCPPubSubSubscriptionSource,
)
from buildflow.core.io.utils.schemas import converters
from buildflow.core.strategies.source import AckInfo, PullResponse, SourceStrategy
from buildflow.core.types.portable_types import FileChangeEvent


@dataclasses.dataclass
class GCSFileChangeEvent(FileChangeEvent):
    storage_client: storage.Client

    @property
    def blob(self) -> bytes:
        if self.metadata["eventType"] == "OBJECT_DELETE":
            raise ValueError("Can't fetch blob for `OBJECT_DELETE` event.")
        bucket = self.storage_client.bucket(bucket_name=self.metadata["bucketId"])
        blob = bucket.get_blob(self.metadata["objectId"])
        return blob.download_as_bytes()


class GCSFileChangeStreamSource(SourceStrategy):
    def __init__(
        self,
        *,
        project_id: str,
        credentials: GCPCredentials,
        pubsub_source: GCPPubSubSubscriptionSource,
    ):
        super().__init__(
            credentials=credentials, strategy_id="gcp-gcs-filestream-source"
        )
        # configuration
        self.pubsub_source = pubsub_source
        clients = gcp_clients.GCPClients(
            credentials=credentials,
            quota_project_id=project_id,
        )
        self.storage_client = clients.get_storage_client(project_id)

    async def pull(self) -> PullResponse:
        pull_response = await self.pubsub_source.pull()
        payload = [
            GCSFileChangeEvent(
                metadata=payload.attributes, storage_client=self.storage_client
            )
            for payload in pull_response.payload
        ]
        return PullResponse(payload=payload, ack_info=pull_response.ack_info)

    async def ack(self, ack_info: AckInfo, success: bool):
        return await self.pubsub_source.ack(ack_info=ack_info, success=success)

    async def backlog(self) -> int:
        return await self.pubsub_source.backlog()

    def max_batch_size(self) -> int:
        return self.pubsub_source.max_batch_size()

    def pull_converter(self, type_: Optional[Type]) -> Callable[[bytes], Any]:
        return converters.identity()
