import enum

from buildflow.core.types.portable_types import (
    TableName,
    TableID,
    TopicID,
    BucketName,
    PortableFileChangeEventType,
)

# TODO: Add comments to show the str patterns
# Optional TODO: Add post-init validation on the str format


# Project Level Types
GCPProjectID = str

GCPRegion = str

GCPZone = str

# BigQuery Types
BigQueryDatasetName = str

BigQueryTableName = TableName

BigQueryTableID = TableID

# PubSub Types
PubSubSubscriptionName = str

PubSubSubscriptionID = str

PubSubTopicName = str

PubSubTopicID = TopicID

# Google Cloud Storage Types
GCSBucketName = BucketName

GCSBucketURL = str


class GCSChangeStreamEventType(enum.Enum):
    OBJECT_FINALIZE = "created"
    OBJECT_DELETE = "deleted"
    OBJECT_ARCHIVE = "archive"
    OBJECT_METADATA_UPDATE = "metadata_update"

    @classmethod
    def from_portable_type(cls, portable_type: PortableFileChangeEventType):
        try:
            return cls(portable_type.value)
        except ValueError:
            raise ValueError(
                "Cannot convert portable file event type to local file "
                f"event type: {portable_type}"
            ) from None
