from dataclasses import dataclass

from buildflow import utils
from buildflow.io.providers.gcp.bigquery import StreamingBigQueryProvider
from buildflow.io.providers.gcp.gcp_pub_sub import GCPPubSubSubscriptionProvider
from buildflow.io.providers.gcp.gcs_file_stream import GCSFileStreamProvider


class ResourceType:
    def provider(self):
        raise NotImplementedError("provider not implemented")


@dataclass
class GCPPubSubSubscription(ResourceType):
    billing_project_id: str
    topic_id: str  # format: projects/{project_id}/topics/{topic_name}
    subscription_name: str = f"buildflow_subscription_{utils.uuid(max_len=6)}"

    def provider(self):
        batch_size = 1000
        return GCPPubSubSubscriptionProvider(
            billing_project_id=self.billing_project_id,
            topic_id=self.topic_id,
            subscription_name=self.subscription_name,
            batch_size=batch_size,
        )


@dataclass
class BigQueryTable(ResourceType):
    table_id: str

    def provider(self):
        billing_project_id = self.table_id.split(".")[0]
        return StreamingBigQueryProvider(
            billing_project_id=billing_project_id, table_id=self.table_id
        )


@dataclass
class GCSFileStream(ResourceType):
    bucket_name: str
    project_id: str

    def provider(self):
        return GCSFileStreamProvider(
            bucket_name=self.bucket_name, project_id=self.project_id
        )


@dataclass
class EmptySink:
    pass
