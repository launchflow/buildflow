from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Type

from google.api_core import exceptions
import pulumi
import pulumi_gcp

from buildflow.io.providers import (
    PullProvider,
    PulumiProvider,
)
from buildflow.io.providers.base import PullResponse, PulumiResources
from buildflow.io.providers.gcp import gcp_pub_sub
from buildflow.io.providers.gcp.utils import clients as gcp_clients
from buildflow.io.providers.schemas import converters


@dataclass
class GCSFileEvent:
    # Metadata about that action taken.
    metadata: Dict[str, Any]
    billing_project: str

    @property
    def blob(self) -> bytes:
        if self.metadata["eventType"] == "OBJECT_DELETE":
            raise ValueError("Can't fetch blob for `OBJECT_DELETE` event.")
        client = gcp_clients.get_storage_client(self.billing_project)
        bucket = client.bucket(bucket_name=self.metadata["bucketId"])
        blob = bucket.get_blob(self.metadata["objectId"])
        return blob.download_as_bytes()


class GCSFileStreamProvider(PullProvider, PulumiProvider):
    def __init__(
        self,
        *,
        bucket_name: str,
        project_id: str,
        event_types: Optional[List[str]] = ("OBJECT_FINALIZE",),
        force_destroy: bool = False,
    ):
        """
        Args:
            bucket_name: The name of the bucket to stream from.
            project_id: The project id that the bucket is in.
            pubsub_topic: The pubsub topic that is configured to point at the bucket.
                If not set we will create one.
            pubsub_subscription: The pubsub subscription that is configure to point at
                the bucket. If not set we will create one.
            event_types: The event types that should trigger the stream. This is only
                used if we are attaching a new notification listener to your bucket.
                Defaults to only new uploads. If set to None will listen to all events.
            force_destroy: If set to True will delete contents of the bucket on destroy.

        """
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.event_types = list(event_types)
        self.force_destroy = force_destroy
        self._managed_publisher = True
        self._managed_subscriber = True
        self._topic_name = f"{self.bucket_name}_notifications"
        self._subscription_name = f"{self.bucket_name}_subscriber"
        self._pubsub_topic = f"projects/{self.project_id}/topics/{self._topic_name}"
        self.subscription_provider = gcp_pub_sub.GCPPubSubSubscriptionProvider(
            project_id=self.project_id,
            topic_id=self._pubsub_topic,
            subscription_name=self._subscription_name,
            batch_size=1000,
            include_attributes=True,
        )

    async def pull(self) -> PullResponse:
        pull_response = await self.subscription_provider.pull()
        payload = [
            GCSFileEvent(metadata=payload.attributes, billing_project=self.project_id)
            for payload in pull_response.payload
        ]
        return PullResponse(payload=payload, ack_info=pull_response.ack_info)

    # TODO: This should not be Optional (goes against Pullable base class)
    async def backlog(self) -> Optional[int]:
        return await self.subscription_provider.backlog()

    async def ack(self, ack_info, success: bool):
        return await self.subscription_provider.ack(ack_info, success)

    def pull_converter(self, user_defined_type: Type) -> Callable[[Any], Any]:
        if (
            not issubclass(user_defined_type, GCSFileEvent)
            and user_defined_type is not None
        ):
            raise ValueError("Input type for GCS file stream should be: `GCSFileEvent`")
        return converters.identity()

    def pulumi(self, type_: Optional[Type]) -> PulumiResources:
        # TODO: should support additional parameters for all resource creation.
        # probably most importantly the bucket (location, ttl, etc..)
        resources = []
        exports = {}
        topic_resource = pulumi_gcp.pubsub.Topic(
            self._topic_name, name=self._topic_name, project=self.project_id
        )

        resources.append(topic_resource)
        exports["gcp.pubsub.topic.name"] = topic_resource.name

        gcs_account = pulumi_gcp.storage.get_project_service_account(
            project=self.project_id, user_project=self.project_id
        )
        binding = pulumi_gcp.pubsub.TopicIAMBinding(
            "binding",
            opts=pulumi.ResourceOptions(depends_on=[topic_resource]),
            topic=self._pubsub_topic,
            role="roles/pubsub.publisher",
            members=[f"serviceAccount:{gcs_account.email_address}"],
        )
        resources.append(binding)

        subscription_resource = pulumi_gcp.pubsub.Subscription(
            self._subscription_name,
            opts=pulumi.ResourceOptions(depends_on=[topic_resource]),
            name=self._subscription_name,
            topic=self._pubsub_topic,
            project=self.project_id,
            ack_deadline_seconds=10 * 60,
            message_retention_duration="1200s",
        )

        resources.append(subscription_resource)
        exports["gcp.pubsub.subscription.name"] = subscription_resource.name

        exports["gcp.pubsub.subscription.name"] = subscription_resource.name

        notification_depends = [topic_resource, binding]

        # First check if the bucket exists.
        try:
            gcp_clients.get_storage_client(self.project_id).get_bucket(self.bucket_name)
        except (exceptions.Forbidden, exceptions.NotFound):
            bucket = pulumi_gcp.storage.Bucket(
                self.bucket_name,
                name=self.bucket_name,
                location="US",
                project=self.project_id,
                force_destroy=self.force_destroy,
            )
            resources.append(bucket)
            exports["gcp.storage.bucket.name"] = self.bucket_name
            notification_depends.append(bucket)

        notification = pulumi_gcp.storage.Notification(
            f"{self.bucket_name}_notification",
            opts=pulumi.ResourceOptions(depends_on=notification_depends),
            payload_format="JSON_API_V1",
            bucket=self.bucket_name,
            event_types=self.event_types,
            topic=self._pubsub_topic,
        )

        resources.append(notification)
        return PulumiResources(resources=resources, exports=exports)
