from typing import Any, Callable, Type

from google.cloud.pubsub_v1.types import PubsubMessage as GCPPubSubMessage

from buildflow.core import utils
from buildflow.core.io.providers.gcp import gcp_clients
from buildflow.core.io.schemas import converters
from buildflow.core.strategies.sink import Batch, SinkStrategy


async def _push_to_topic(client, topic: str, batch: Batch):
    pubsub_messages = [GCPPubSubMessage(data=elem) for elem in batch]
    await client.publish(topic=topic, messages=pubsub_messages)


class GCPPubSubTopicSink(SinkStrategy):
    def __init__(self, *, project_id: str, topic_name: str):
        self.project_id = project_id
        self.topic_name = topic_name
        self.publisher_client = gcp_clients.get_async_publisher_client(project_id)

    @property
    def topic_id(self):
        return f"projects/{self.project_id}/topics/{self.topic_name}"

    @utils.log_errors(endpoint="apis.buildflow.dev/...")
    async def push(self, batch: Batch):
        await _push_to_topic(self.publisher_client, self.topic_id, batch)

    def push_converter(self, user_defined_type: Type) -> Callable[[Any], Any]:
        return converters.bytes_push_converter(user_defined_type)
