import dataclasses

from buildflow.core.types.portable_types import QueueID
from buildflow.io.primitive import PortablePrimtive


@dataclasses.dataclass
class Queue(PortablePrimtive):
    queue_id: QueueID
