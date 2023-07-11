import logging

import google.auth
from google.auth import exceptions
from google.auth.credentials import Credentials
from google.cloud import (
    bigquery,
    bigquery_storage_v1,
    monitoring_v3,
    pubsub,
    storage,
)
from google.pubsub_v1.services.publisher import PublisherAsyncClient
from google.pubsub_v1.services.subscriber import SubscriberAsyncClient
from google.cloud.bigquery_storage_v1.services.big_query_write.async_client import (
    BigQueryWriteAsyncClient,
)  # noqa: E501
from google.api_core import client_options

_CREDS = None


def _get_gcp_creds(quota_project_id: str) -> Credentials:
    global _CREDS
    if _CREDS is None:
        try:
            _CREDS, _ = google.auth.default(quota_project_id=quota_project_id)
        except exceptions.DefaultCredentialsError:
            # if we failed to fetch the credentials fall back to anonymous
            # credentials. This shouldn't normally happen, but can happen if
            # user is running on a machine with now default creds.
            logging.warning("no default credentials found, using anonymous credentials")
            _CREDS = google.auth.credentials.AnonymousCredentials()
    return _CREDS


def get_storage_client(project: str = None) -> storage.Client:
    creds = _get_gcp_creds(project)
    return storage.Client(credentials=creds, project=project)


def get_bigquery_client(project: str = None) -> bigquery.Client:
    creds = _get_gcp_creds(project)
    return bigquery.Client(credentials=creds, project=project)


def get_bigquery_write_async_client(
    project: str = None,
) -> bigquery_storage_v1.BigQueryWriteClient:
    creds = _get_gcp_creds(project)
    return BigQueryWriteAsyncClient(
        credentials=creds,
        client_options=client_options.ClientOptions(quota_project_id=project),
    )


def get_bigquery_storage_client(
    project: str = None,
) -> bigquery_storage_v1.BigQueryReadClient:
    creds = _get_gcp_creds(project)
    return bigquery_storage_v1.BigQueryReadClient(credentials=creds)


def get_metrics_client(project: str):
    creds = _get_gcp_creds(project)
    return monitoring_v3.MetricServiceClient(credentials=creds)


def get_async_subscriber_client(project: str):
    creds = _get_gcp_creds(project)
    return SubscriberAsyncClient(credentials=creds)


def get_async_publisher_client(project: str):
    creds = _get_gcp_creds(project)
    return PublisherAsyncClient(credentials=creds)


def get_publisher_client(project: str):
    creds = _get_gcp_creds(project)
    return pubsub.PublisherClient(credentials=creds)


def get_subscriber_client(project: str):
    creds = _get_gcp_creds(project)
    return pubsub.SubscriberClient(credentials=creds)
