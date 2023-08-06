import enum
from dataclasses import dataclass
from typing import Any, Dict

from buildflow.core.types.shared_types import FilePath


class FileFormat(enum.Enum):
    # TODO: Support additional file formats (Arrow, Avro, etc..)
    PARQUET = "parquet"
    CSV = "csv"
    JSON = "json"


class PortableFileChangeEventType(enum.Enum):
    CREATED = "created"
    DELETED = "portable"
    UNKNOWN = "unknown"


@dataclass
class FileChangeEvent:
    file_path: FilePath
    event_type: PortableFileChangeEventType

    # Metadata specific to the cloud provider.
    metadata: Dict[str, Any]

    @property
    def blob(self) -> bytes:
        raise NotImplementedError(f"blob not implemented for {type(self)}")
