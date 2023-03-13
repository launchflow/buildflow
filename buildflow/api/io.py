import dataclasses
import inspect
import logging
from typing import Any, Dict, List, TypeVar

from google.api_core import exceptions
from google.cloud import bigquery, pubsub, storage

from buildflow.api.schemas import bigquery as bq_schemas


class InputOutput:
    """Super class for all input and output types."""

    def setup(self, source: bool, sink: bool,
              process_arg_spec: inspect.FullArgSpec):
        """Perform any setup that is needed to connect to a resource.

        Args:
            source: Whether or not this is being used as an input.
            sink: Whther or not this is being used as an output.
        """


IO = TypeVar('IO', bound=InputOutput)


@dataclasses.dataclass(frozen=True)
class HTTPEndpoint(InputOutput):
    host: str = 'localhost'
    port: int = 3569


@dataclasses.dataclass(frozen=True)
class PubSub(InputOutput):
    topic: str = ''
    subscription: str = ''

    def setup(self, source: bool, sink: bool,
              process_arg_spec: inspect.FullArgSpec):
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
                        'subscription if needed.')


@dataclasses.dataclass(frozen=True)
class BigQuery(InputOutput):

    # The BigQuery table to read or write from.
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

    def setup(self, source: bool, sink: bool,
              process_arg_spec: inspect.FullArgSpec):
        client = bigquery.Client()
        if source:
            if self.table_id:
                try:
                    client.get_table(table=self.table_id)
                except Exception:
                    raise ValueError(
                        f'Failed to retrieve BigQuery table: {self.table_id} '
                        'for reading. Please ensure this table exists and you '
                        'have access.')
            if self.query:
                try:
                    client.query(
                        self.query,
                        job_config=bigquery.QueryJobConfig(dry_run=True))
                except Exception as e:
                    raise ValueError(
                        f'Failed to test BigQuery query. Failed with: {e}')
        if sink:
            if 'return' in process_arg_spec.annotations:
                return_type = process_arg_spec.annotations['return']
                if not dataclasses.is_dataclass(return_type):
                    print('Output type was not a dataclass cannot validate '
                          'schema.')
                schema = bq_schemas.dataclass_to_bq_schema(
                    dataclasses.fields(process_arg_spec.annotations['return']))
                schema.sort(key=lambda sf: sf.name)
                try:
                    table = client.get_table(table=self.table_id)
                    bq_schema = table.schema
                    bq_schema.sort(key=lambda sf: sf.name)
                    if schema != bq_schema:
                        only_in_bq = set(bq_schema) - set(schema)
                        only_in_pytype = set(schema) - set(bq_schema)
                        error_str = [
                            'Output schema did not match table schema.'
                        ]
                        if only_in_bq:
                            error_str.append(
                                'Fields found only in BQ schema:\n'
                                f'{bq_schemas.schema_fields_to_str(only_in_bq)}'  # noqa: E501
                            )
                        if only_in_pytype:
                            error_str.append(
                                'Fields found only in PyType schema:\n'
                                f'{bq_schemas.schema_fields_to_str(only_in_pytype)}'  # noqa: E501
                            )
                        raise ValueError('\n'.join(error_str))
                except exceptions.NotFound:
                    dataset_ref = '.'.join(self.table_id.split('.')[0:2])
                    client.create_dataset(dataset_ref, exists_ok=True)
                    table = client.create_table(
                        bigquery.Table(self.table_id, schema))
                except exceptions.PermissionDenied:
                    raise ValueError(
                        f'Failed to retrieve BigQuery table: {self.table_id} '
                        'for writing. Please ensure this table exists and you '
                        'have access.')
            else:
                print(
                    'No output type provided. Cannot validate BigQuery table.')


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


@dataclasses.dataclass(frozen=False)
class GCSFileEventStream(InputOutput):
    bucket_name: str
    project_id: str
    pubsub_topic: str = ''
    pubsub_subscription: str = ''

    def __post_init__(self):
        if not self.pubsub_topic:
            self.pubsub_topic = f'projects/{self.project_id}/topics/{self.bucket_name}_notifications'  # noqa: E501
            logging.info(
                f'No pubsub topic provided. Defaulting to {self.pubsub_topic}.'
            )
        if not self.pubsub_subscription:
            self.pubsub_subscription = f'projects/{self.project_id}/subscriptions/{self.bucket_name}_subscriber'  # noqa: E501
            logging.info('No pubsub subscription provided. Defaulting to '
                         f'{self.pubsub_subscription}.')

    def setup(self, source: bool, sink: bool,
              process_arg_spec: inspect.FullArgSpec):
        # Create the topic if it doesn't exist.
        publisher_client = pubsub.PublisherClient()
        try:
            publisher_client.get_topic(topic=self.pubsub_topic)
        except exceptions.NotFound:
            print(f'topic {self.pubsub_topic} not found attempting to create')
            try:
                print(f'Creating topic: {self.pubsub_topic}')
                publisher_client.create_topic(name=self.pubsub_topic)
            except exceptions.PermissionDenied:
                raise ValueError(
                    f'Failed to create topic: {self.pubsub_topic}. Please '
                    'ensure you have permission to read the existing topic or '
                    'permission to create a new topic if needed.')
        subscriber_client = pubsub.SubscriberClient()
        try:
            subscriber_client.get_subscription(
                subscription=self.pubsub_subscription)
        except exceptions.NotFound:
            try:
                print(f'Creating subscription: {self.pubsub_subscription}')
                subscriber_client.create_subscription(
                    name=self.pubsub_subscription, topic=self.pubsub_topic)
            except exceptions.PermissionDenied:
                raise ValueError(
                    f'Failed to create subscription: '
                    f'{self.pubsub_subscription}. Please ensure you have '
                    'permission to read the existing subscription or '
                    'permission to create a new subscription if needed.')
        # Create the bucket notifications if they don't exist.
        storage_client = storage.Client()
        bucket = None
        try:
            bucket = storage_client.get_bucket(self.bucket_name)
        except exceptions.NotFound:
            print(f'bucket {self.bucket_name} not found attempting to create')
            try:
                print(f'Creating bucket: {self.bucket_name}')
                bucket = storage_client.create_bucket(self.bucket_name,
                                                      project=self.project_id)
            except exceptions.PermissionDenied:
                raise ValueError(
                    f'Failed to create bucket: {self.bucket_name}. Please '
                    'ensure you have permission to read the existing bucket '
                    'or permission to create a new bucket if needed.')

        notification_found = False
        try:
            _, _, _, topic_name = self.pubsub_topic.split('/')
            # gotcha 1: have to list through notifications to get the topic
            notifications = bucket.list_notifications()
            for notification in notifications:
                if (notification.topic_name == topic_name
                        and notification.bucket.name == self.bucket_name):
                    notification_found = True
                    break

        except exceptions.PermissionDenied:
            raise ValueError(
                'Failed to create bucket notification for bucket: '
                f'{self.bucket_name}. Please ensure you have permission '
                'to modify the bucket.')
        if not notification_found:
            print(f'bucket notification for bucket {self.bucket_name} not '
                  'found attempting to create')
            try:
                print(f'Creating notification for bucket {self.bucket_name}')
                _, project, _, topic = self.pubsub_topic.split('/')
                # gotcha 2: you cant pass the full topic path
                bucket.notification(topic_name=topic,
                                    topic_project=project).create()
            except exceptions.PermissionDenied:
                raise ValueError(
                    'Failed to create bucket notification for bucket: '
                    f'{self.bucket_name}. Please ensure you have permission '
                    'to modify the bucket.')
