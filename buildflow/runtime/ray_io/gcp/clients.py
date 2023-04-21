import google.auth
from google.auth.credentials import Credentials
from google.cloud import (
    bigquery,
    bigquery_storage_v1,
    monitoring_v3,
    pubsub,
    storage,
)
from google.pubsub_v1.services.subscriber import SubscriberAsyncClient


def _get_gcp_creds(quota_project_id: str) -> Credentials:
    creds, _ = google.auth.default(quota_project_id=quota_project_id)
    return creds


def get_storage_client(project: str = None) -> storage.Client:
    creds = _get_gcp_creds(project)
    return storage.Client(credentials=creds, project=project)


def get_bigquery_client(project: str = None) -> bigquery.Client:
    creds = _get_gcp_creds(project)
    return bigquery.Client(credentials=creds, project=project)


def get_bigquery_storage_client(
        project: str = None) -> bigquery_storage_v1.BigQueryReadClient:
    creds = _get_gcp_creds(project)
    return bigquery_storage_v1.BigQueryReadClient(credentials=creds)


def get_metrics_client(project: str):
    creds = _get_gcp_creds(project)
    return monitoring_v3.MetricServiceClient(credentials=creds)


def get_async_subscriber_client(project: str):
    creds = _get_gcp_creds(project)
    return SubscriberAsyncClient(credentials=creds)


def get_publisher_client(project: str):
    creds = _get_gcp_creds(project)
    return pubsub.PublisherClient(credentials=creds)


def get_subscriber_client(project: str):
    creds = _get_gcp_creds(project)
    return pubsub.SubscriberClient(credentials=creds)
