import dataclasses
from typing import Any, Dict, List, TypeVar

from google.api_core import exceptions
from google.cloud import pubsub


class InputOutput:
    """Super class for all input and output types."""

    def setup(self):
        """Perform any setup that is needed to connect to a resource."""


IO = TypeVar('IO', bound=InputOutput)


@dataclasses.dataclass(frozen=True)
class HTTPEndpoint(InputOutput):
    host: str = 'localhost'
    port: int = 3569


@dataclasses.dataclass(frozen=True)
class PubSub(InputOutput):
    topic: str = ''
    subscription: str = ''

    def setup(self):
        if self.topic:
            publisher_client = pubsub.PublisherClient()
            try:
                publisher_client.get_topic(topic=self.topic)
            except exceptions.NotFound:
                print(f'topic {self.topic} not found attempting to create')
                try:
                    print(f'Creating topic: {self.topic}')
                    publisher_client.create_topic(name=self.topic)
                except exceptions.PermissionDenied:
                    raise ValueError(
                        f'Failed to create topic: {self.topic}. Please ensure '
                        'you have permission to read the existing topic or '
                        'permission to create a new topic if needed.')
        if self.subscription:
            subscriber_client = pubsub.SubscriberClient()
            try:
                subscriber_client.get_subscription(
                    subscription=self.subscription)
            except exceptions.NotFound:
                if not self.topic:
                    raise ValueError(
                        f'subscription: {self.subscription} was not found, '
                        'please provide the topic so we can create the '
                        'subscriber or ensure you have read access to the '
                        'subscribe.')
                try:
                    print(f'Creating subscription: {self.subscription}')
                    subscriber_client.create_subscription(
                        name=self.subscription, topic=self.topic)
                except exceptions.PermissionDenied:
                    raise ValueError(
                        f'Failed to create subscription: {self.subscription}. '
                        'Please ensure you have permission to read the '
                        'existing subscription or permission to create a new '
                        'subscription if needed.'
                    )


@dataclasses.dataclass(frozen=True)
class BigQuery(InputOutput):

    # The BigQuery table to read from.
    # Should be of the format project.dataset.table
    table_id: str = ''
    # The query to read data from.
    query: str = ''
    # The temporary dataset to store query results in. If unspecified we will
    # attempt to create one.
    temp_dataset: str = ''
    # The billing project to use for query usage. If unset we will use the
    # project configured with application default credentials.
    billing_project: str = ''
    # The temporary gcs bucket uri to store temp data in. If unspecified we
    # will attempt to create one.
    temp_gcs_bucket: str = ''


@dataclasses.dataclass(frozen=True)
class RedisStream(InputOutput):
    host: str
    port: str
    streams: List[str]
    start_positions: Dict[str, str] = dataclasses.field(default_factory=dict)
    # Read timeout. If > 0 this is how long we will read from the redis stream.
    read_timeout_secs: int = -1


@dataclasses.dataclass(frozen=True)
class DuckDB(InputOutput):
    database: str
    table: str = ''
    query: str = ''


@dataclasses.dataclass(frozen=True)
class Empty(InputOutput):
    inputs: List[Any] = dataclasses.field(default_factory=list)
