import dataclasses

from buildflow.core.io.primitive import PortablePrimtive
from buildflow.core.types.portable_types import QueueID


@dataclasses.dataclass
class Queue(PortablePrimtive):
    queue_id: QueueID
