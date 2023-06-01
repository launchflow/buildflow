from dataclasses import dataclass
from typing import Iterable, Any

from buildflow.io.providers.pulsing_provider import PulsingProvider
from buildflow.io.providers.gcp.gcp_pub_sub import GCPPubSubProvider
from buildflow.io.providers.gcp.bigquery import StreamingBigQueryProvider
from buildflow.io.providers.gcp.gcs_file_stream import GCSFileStreamProvider


@dataclass
class GCPPubSubSubscription:
    """A reference that pulls items from a GCP Pub/Sub subscription."""

    topic_id: str
    subscription_id: str

    def provider(self):
        # 'projects/daring-runway-374503/subscriptions/taxiride-sub')
        billing_project_id = self.subscription_id.split("/")[1]
        batch_size = 1000
        return GCPPubSubProvider(
            billing_project_id=billing_project_id,
            topic_id=self.topic_id,
            subscription_id=self.subscription_id,
            batch_size=batch_size,
        )


@dataclass
class BigQueryTable:
    """A reference that pushes items to a BigQuery table."""

    table_id: str

    def provider(self):
        billing_project_id = self.table_id.split(".")[0]
        return StreamingBigQueryProvider(
            billing_project_id=billing_project_id, table_id=self.table_id
        )


@dataclass
class GCSFileStream:
    """A reference that emits items when a new filed is uploaded to a bucket."""

    bucket_name: str
    project_id: str

    def provider(self):
        return GCSFileStreamProvider(
            bucket_name=self.bucket_name, project_id=self.project_id
        )


@dataclass
class Pulse:
    """A reference that emits items at a given interval.

    Once the end of the items is reached, it will start again from the beginning.
    """

    items: Iterable[Any]
    pulse_interval_seconds: float

    def provider(self):
        return PulsingProvider(
            items=self.items, pulse_interval_seconds=self.pulse_interval_seconds
        )


@dataclass
class EmptySink:
    pass
