import enum

from buildflow.types.portable import PortableFileChangeEventType


class FileChangeStreamEventType(enum.Enum):
    CREATED = "created"
    DELETED = "deleted"
    MODIFIED = "modified"
    MOVED = "moved"

    @classmethod
    def from_portable_type(cls, portable_type: PortableFileChangeEventType):
        try:
            return cls(portable_type.value)
        except ValueError:
            raise ValueError(
                "Cannot convert portable file event type to local file "
                f"event type: {portable_type}"
            ) from None
