import dataclasses
import enum

from google.cloud import storage

from buildflow.types.portable import FileChangeEvent, PortableFileChangeEventType


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


@dataclasses.dataclass
class GCSFileChangeEvent(FileChangeEvent):
    event_type: GCSChangeStreamEventType
    storage_client: storage.Client

    @property
    def blob(self) -> bytes:
        if self.metadata["eventType"] == "OBJECT_DELETE":
            raise ValueError("Can't fetch blob for `OBJECT_DELETE` event.")
        bucket = self.storage_client.bucket(bucket_name=self.metadata["bucketId"])
        blob = bucket.get_blob(self.metadata["objectId"])
        return blob.download_as_bytes()
