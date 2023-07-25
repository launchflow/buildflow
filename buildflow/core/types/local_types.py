import enum

from buildflow.core.types.portable_types import PortableFileChangeEventType


class FileFormat(enum.Enum):
    # TODO: Support additional file formats (Arrow, Avro, etc..)
    PARQUET = "parquet"
    CSV = "csv"
    JSON = "json"


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


# DuckDB

DuckDBDatabase = str
DuckDBTable = str
