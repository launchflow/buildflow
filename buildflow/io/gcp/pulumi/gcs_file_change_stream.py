from typing import Iterable

import pulumi
import pulumi_gcp

from buildflow.core.credentials import GCPCredentials
from buildflow.io.gcp.pubsub_subscription import GCPPubSubSubscription
from buildflow.io.gcp.storage import GCSBucket
from buildflow.types.gcp import GCSChangeStreamEventType


class GCSFileChangeStreamPulumiResource(pulumi.ComponentResource):
    def __init__(
        self,
        bucket: GCSBucket,
        subscription: GCPPubSubSubscription,
        event_types: Iterable[GCSChangeStreamEventType],
        # pulumi_resource options (buildflow internal concept)
        credentials: GCPCredentials,
        opts: pulumi.ResourceOptions,
    ):
        super().__init__(
            "buildflow:gcp:storage:GCSFileChangeStream",
            f"buildflow-{bucket.project_id}-{bucket.bucket_name}",
            None,
            opts,
        )

        gcs_account = pulumi_gcp.storage.get_project_service_account(
            project=bucket.project_id,
            user_project=bucket.project_id,
        )
        self.binding = pulumi_gcp.pubsub.TopicIAMBinding(
            f"{bucket.bucket_name}-{subscription.topic.topic_name}_binding",
            opts=pulumi.ResourceOptions(parent=self),
            topic=subscription.topic.topic_name,
            role="roles/pubsub.publisher",
            project=subscription.topic.project_id,
            members=[f"serviceAccount:{gcs_account.email_address}"],
        )

        self.notification = pulumi_gcp.storage.Notification(
            f"{bucket.bucket_name}_notification",
            opts=pulumi.ResourceOptions(parent=self, depends_on=[self.binding]),
            bucket=bucket.bucket_name,
            topic=subscription.topic.topic_id,
            payload_format="JSON_API_V1",
            event_types=[et.name for et in event_types],
        )

        self.register_outputs(
            {
                "gcp.storage_notification.notification.": self.notification.id,
                "gcp.pubsub.topic.binding": self.binding.topic,
            }
        )
