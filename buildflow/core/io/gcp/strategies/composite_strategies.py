from typing import Any, Callable, Optional, Type

from buildflow.core.options.runtime_options import RuntimeOptions
from buildflow.core.io.gcp.strategies.pubsub_strategies import (
    GCPPubSubSubscriptionSource,
)
from buildflow.core.strategies.source import AckInfo, PullResponse, SourceStrategy


class GCSFileStreamSource(SourceStrategy):
    def __init__(
        self,
        *,
        runtime_options: RuntimeOptions,
        pubsub_source: GCPPubSubSubscriptionSource,
    ):
        super().__init__(
            runtime_options=runtime_options, strategy_id="gcp-gcs-filestream-source"
        )
        # configuration
        self.pubsub_source = pubsub_source

    async def pull(self) -> PullResponse:
        return await self.pubsub_source.pull()

    async def ack(self, ack_info: AckInfo, success: bool):
        return await self.pubsub_source.ack(ack_info=ack_info, success=success)

    async def backlog(self) -> int:
        return await self.pubsub_source.backlog()

    def max_batch_size(self) -> int:
        return self.pubsub_source.max_batch_size()

    def pull_converter(self, type_: Optional[Type]) -> Callable[[bytes], Any]:
        raise NotImplementedError(
            "TODO: Implement pull_converter for GCSFileStreamSource"
        )
