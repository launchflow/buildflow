from dataclasses import dataclass
from typing import Any, Iterable, Optional, Union

from buildflow import utils
from buildflow.io.providers.file_provider import FileFormat, FileProvider
from buildflow.io.providers.gcp.bigquery import StreamingBigQueryProvider
from buildflow.io.providers.gcp.gcp_pub_sub import (
    GCPPubSubSubscriptionProvider,
    GCPPubSubTopicProvider,
)
from buildflow.io.providers.gcp.gcs_file_stream import GCSFileStreamProvider
from buildflow.io.providers.pulsing_provider import PulsingProvider


class ResourceType:
    def provider(self):
        raise NotImplementedError("provider not implemented")


@dataclass
class GCPPubSubTopic(ResourceType):
    project_id: str
    topic_name: Optional[None]

    def __post_init__(self):
        if self.topic_name is None:
            project_hash = utils.stable_hash(self.project_id)
            self.topic_name = f"buildflow_topic_{project_hash[:8]}"

    def provider(self):
        return GCPPubSubTopicProvider(
            project_id=self.project_id, topic_name=self.topic_name
        )


@dataclass
class GCPPubSubSubscription(ResourceType):
    project_id: str
    topic_id: str  # format: projects/{project_id}/topics/{topic_name}
    subscription_name: Optional[str] = None

    def __post_init__(self):
        if self.subscription_name is None:
            topic_hash = utils.stable_hash(self.topic_id)
            self.subscription_name = f"buildflow_subscription_{topic_hash[:8]}"

    def provider(self):
        batch_size = 1000
        return GCPPubSubSubscriptionProvider(
            project_id=self.project_id,
            topic_id=self.topic_id,
            subscription_name=self.subscription_name,
            batch_size=batch_size,
        )


@dataclass
class BigQueryTable(ResourceType):
    table_id: str

    # Resource management options
    include_dataset: bool = True
    destroy_protection: bool = True

    def provider(self):
        project_id = self.table_id.split(".")[0]
        return StreamingBigQueryProvider(
            project_id=project_id,
            table_id=self.table_id,
            include_dataset=self.include_dataset,
            destroy_protection=self.destroy_protection,
        )


@dataclass
class GCSFileStream(ResourceType):
    bucket_name: str
    project_id: str

    # Resource management options
    force_destroy: bool = False

    def provider(self):
        return GCSFileStreamProvider(
            bucket_name=self.bucket_name,
            project_id=self.project_id,
            force_destroy=self.force_destroy,
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
class Files:
    """A reference that emits items to files"""

    file_path: str
    file_format: Union[str, FileFormat]

    def provider(self):
        return FileProvider(file_path=self.file_path, file_format=self.file_format)


@dataclass
class EmptySink:
    pass
