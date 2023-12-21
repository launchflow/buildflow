import dataclasses
import enum
from typing import Any
from urllib.parse import unquote_plus

from buildflow.core.types.aws_types import S3BucketName
from buildflow.types.portable import FileChangeEvent, PortableFileChangeEventType


class S3ChangeStreamEventType(enum.Enum):
    OBJECT_CREATED_ALL = "ObjectCreated:*"
    OBJECT_CREATED_PUT = "ObjectCreated:Put"
    OBJECT_CREATED_POST = "ObjectCreated:Post"
    OBJECT_CREATED_COPY = "ObjectCreated:Copy"
    OBJECT_CREATED_COMPLETE_MULTIPART_UPLOAD = "ObjectCreated:CompleteMultipartUpload"
    OBJECT_REMOVED_ALL = "ObjectRemoved:*"
    OBJECT_REMOVED_DELETE = "ObjectRemoved:Delete"
    OBJECT_REMOVED_DELETE_MARKER_CREATED = "ObjectRemoved:DeleteMarkerCreated"
    UNKNOWN = "unknown"

    @classmethod
    def create_event_types(cls):
        return [
            cls.OBJECT_CREATED_ALL,
            cls.OBJECT_CREATED_PUT,
            cls.OBJECT_CREATED_POST,
            cls.OBJECT_CREATED_COPY,
            cls.OBJECT_CREATED_COMPLETE_MULTIPART_UPLOAD,
        ]

    @classmethod
    def delete_event_types(cls):
        return [
            cls.OBJECT_REMOVED_ALL,
            cls.OBJECT_REMOVED_DELETE,
            cls.OBJECT_REMOVED_DELETE_MARKER_CREATED,
        ]

    @classmethod
    def from_portable_type(cls, portable_type: PortableFileChangeEventType):
        if portable_type == PortableFileChangeEventType.CREATED:
            return cls.OBJECT_CREATED_ALL
        elif portable_type == PortableFileChangeEventType.DELETED:
            return cls.OBJECT_REMOVED_ALL
        raise ValueError(
            "Cannot convert portable file event type to aws file "
            f"event type: {portable_type}"
        )

    def to_portable_type(self):
        value = self.value
        if value.startswith("ObjectCreated:"):
            return PortableFileChangeEventType.CREATED
        elif value.startswith("ObjectRemoved:"):
            return PortableFileChangeEventType.DELETED
        return PortableFileChangeEventType.UNKNOWN


@dataclasses.dataclass
class S3FileChangeEvent(FileChangeEvent):
    event_type: S3ChangeStreamEventType
    bucket_name: S3BucketName
    s3_client: Any

    @property
    def blob(self) -> bytes:
        # S3 gives us a url encoded path, so we need to decode it inorder to read
        # the file from s3.
        file_path = unquote_plus(self.file_path)
        data = self.s3_client.get_object(Bucket=self.bucket_name, Key=file_path)
        return data["Body"].read()
