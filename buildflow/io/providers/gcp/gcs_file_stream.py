from dataclasses import asdict, dataclass
import logging
from typing import Any, Callable, Dict, List, Optional, Type

from google.api_core import exceptions

from buildflow.io.providers import (
    PlanProvider,
    PullProvider,
    SetupProvider,
)
from buildflow.io.providers.base import PullResponse
from buildflow.io.providers.gcp import gcp_pub_sub
from buildflow.io.providers.gcp.utils import clients as gcp_clients
from buildflow.io.providers.gcp.utils import setup_utils
from buildflow.io.providers.schemas import converters


@dataclass
class _GCSSourcePlan:
    bucket_name: str
    pubsub_topic: str
    pubsub_subscription: str


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


class GCSFileStreamProvider(PullProvider, SetupProvider, PlanProvider):
    def __init__(
        self,
        *,
        bucket_name: str,
        project_id: str,
        pubsub_topic: str = "",
        pubsub_subscription: str = "",
        event_types: Optional[List[str]] = ("OBJECT_FINALIZE",),
        billing_project: str = "",
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

        """
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.pubsub_topic = pubsub_topic
        self.pubsub_subscription = pubsub_subscription
        self.event_types = event_types
        self.billing_project = billing_project
        self._managed_publisher = True
        self._managed_subscriber = True
        if not self.pubsub_topic:
            self.pubsub_topic = f"projects/{self.project_id}/topics/{self.bucket_name}_notifications"  # noqa: E501
            logging.info(
                f"No pubsub topic provided. Defaulting to {self.pubsub_topic}."
            )
        else:
            self._managed_publisher = False
        if not self.pubsub_subscription:
            self.pubsub_subscription = f"projects/{self.project_id}/subscriptions/{self.bucket_name}_subscriber"  # noqa: E501
            logging.info(
                "No pubsub subscription provided. Defaulting to "
                f"{self.pubsub_subscription}."
            )
        else:
            self._managed_subscriber = False
        if not self.billing_project:
            self.billing_project = self.project_id
        self.pubsub_ref = gcp_pub_sub.GCPPubSubProvider(
            billing_project_id=self.billing_project,
            topic_id=self.pubsub_topic,
            subscription_id=self.pubsub_subscription,
            batch_size=1000,
            include_attributes=True,
        )

    async def pull(self) -> PullResponse:
        payloads, ack_ids = await self.pubsub_ref.pull()
        payloads = [
            GCSFileEvent(
                metadata=payload.attributes, billing_project=self.billing_project
            )
            for payload in payloads
        ]
        return PullResponse(payloads=payloads, ack_ids=ack_ids)

    # TODO: This should not be Optional (goes against Pullable base class)
    async def backlog(self) -> Optional[int]:
        return await self.pubsub_ref.backlog()

    async def ack(self, ack_ids: List[str]):
        return await self.pubsub_ref.ack(ack_ids)

    def pull_converter(self, user_defined_type: Type) -> Callable[[Any], Any]:
        if (
            not issubclass(user_defined_type, GCSFileEvent)
            and user_defined_type is not None
        ):
            raise ValueError("Input type for GCS file stream should be: `GCSFileEvent`")
        return converters.identity()

    async def plan(self) -> Dict[str, Any]:
        return asdict(
            _GCSSourcePlan(
                bucket_name=self.bucket_name,
                pubsub_topic=self.pubsub_topic,
                pubsub_subscription=self.pubsub_subscription,
            )
        )

    async def setup(self) -> bool:
        # TODO: Can we make the pubsub setup easier by just running:
        #   self._pubsub_ref.set()?
        storage_client = gcp_clients.get_storage_client(self.billing_project)
        bucket = None
        try:
            bucket = storage_client.get_bucket(self.bucket_name)
        except exceptions.NotFound:
            print(f"bucket {self.bucket_name} not found attempting to create")
            try:
                print(f"Creating bucket: {self.bucket_name}")
                bucket = storage_client.create_bucket(
                    self.bucket_name, project=self.project_id
                )
            except exceptions.PermissionDenied:
                raise ValueError(
                    f"Failed to create bucket: {self.bucket_name}. Please "
                    "ensure you have permission to read the existing bucket "
                    "or permission to create a new bucket if needed."
                )

        if self._managed_subscriber:
            gcs_notify_sa = f"serviceAccount:service-{bucket.project_number}@gs-project-accounts.iam.gserviceaccount.com"  # noqa: E501
            setup_utils.maybe_create_subscription(
                pubsub_subscription=self.pubsub_subscription,
                pubsub_topic=self.pubsub_topic,
                billing_project=self.billing_project,
                publisher_members=[gcs_notify_sa],
            )
        if self._managed_publisher:
            notification_found = False
            try:
                _, _, _, topic_name = self.pubsub_topic.split("/")
                # gotcha 1: have to list through notifications to get the topic
                notifications = bucket.list_notifications()
                for notification in notifications:
                    if (
                        notification.topic_name == topic_name
                        and notification.bucket.name == self.bucket_name
                    ):
                        notification_found = True
                        break

            except exceptions.PermissionDenied:
                raise ValueError(
                    "Failed to create bucket notification for bucket: "
                    f"{self.bucket_name}. Please ensure you have permission "
                    "to modify the bucket."
                )
            if not notification_found:
                print(
                    f"bucket notification for bucket {self.bucket_name} not "
                    "found attempting to create"
                )
                try:
                    print(f"Creating notification for bucket {self.bucket_name}")
                    _, project, _, topic = self.pubsub_topic.split("/")
                    # gotcha 2: you cant pass the full topic path
                    bucket.notification(
                        topic_name=topic,
                        topic_project=project,
                        event_types=self.event_types,
                    ).create()
                except exceptions.PermissionDenied:
                    raise ValueError(
                        "Failed to create bucket notification for bucket: "
                        f"{self.bucket_name}. Please ensure you have "
                        "permission to modify the bucket."
                    )
