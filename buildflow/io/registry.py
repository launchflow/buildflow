import dataclasses

from buildflow.io.providers.gcp.gcp_pub_sub import GCPPubSubProvider
from buildflow.io.providers.gcp.bigquery import StreamingBigQueryProvider


@dataclasses.dataclass
class GCPPubSubSubscription:
    topic_id: str
    subscription_id: str

    def provider(self):
        # 'projects/daring-runway-374503/subscriptions/taxiride-sub')
        billing_project_id = self.subscription_id.split('/')[1]
        batch_size = 1000
        return GCPPubSubProvider(billing_project_id=billing_project_id,
                                 topic_id=self.topic_id,
                                 subscription_id=self.subscription_id,
                                 batch_size=batch_size)


@dataclasses.dataclass
class BigQueryTable:
    table_id: str

    def provider(self):
        billing_project_id = self.table_id.split('.')[0]
        return StreamingBigQueryProvider(billing_project_id=billing_project_id,
                                         table_id=self.table_id)


@dataclasses.dataclass
class EmptySink:
    pass
