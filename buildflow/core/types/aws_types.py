import enum

from buildflow.core.types.portable_types import PortableFileChangeEventType

SQSQueueName = str

AWSAccountID = str
AWSRegion = str

S3BucketName = str


class S3ChangeStreamEventType(enum.Enum):
    OBJECT_CREATED_ALL = "ObjectCreated:*"
    OBJECT_CREATED_PUT = "ObjectCreated:Put"
    OBJECT_CREATED_POST = "ObjectCreated:Post"
    OBJECT_CREATED_COPY = "ObjectCreated:Copy"
    OBJECT_CREATED_COMPLETE_MULTIPART_UPLOAD = "ObjectCreated:CompleteMultipartUpload"
    OBJECT_REMOVED_ALL = "ObjectRemoved:*"
    OBJECT_REMOVED_DELETE = "ObjectRemoved:Delete"
    OBJECT_REMOVED_DELETE_MARKER_CREATED = "ObjectRemoved:DeleteMarkerCreated"

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
