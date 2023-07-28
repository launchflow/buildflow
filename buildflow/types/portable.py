import enum
from dataclasses import dataclass
from typing import Any, Dict

from buildflow.core.types.shared_types import FilePath


class PortableFileChangeEventType(enum.Enum):
    CREATED = "created"
    DELETED = "portable"
    UNKNOWN = "unknown"


@dataclass
class FileChangeEvent:
    file_path: FilePath
    portable_event_type: PortableFileChangeEventType

    # Metadata specific to the cloud provider.
    metadata: Dict[str, Any]

    @property
    def blob(self) -> bytes:
        raise NotImplementedError(f"blob not implemented for {type(self)}")
