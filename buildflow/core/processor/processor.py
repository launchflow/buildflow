import enum
from typing import Generic, List, TypeVar

from buildflow.core.background_tasks.background_task import BackgroundTask


class ProcessorType(enum.Enum):
    COLLECTOR = "collector"
    CONNECTION = "connection"
    CONSUMER = "consumer"
    ENDPOINT = "endpoint"


ProcessorID = str
GroupID = str


class ProcessorAPI:
    processor_id: ProcessorID
    processor_type: ProcessorType

    def setup(self):
        raise NotImplementedError("setup not implemented")

    async def process(self, *args, **kwargs):
        raise NotImplementedError("process not implemented for Processor")

    def background_tasks(self) -> List[BackgroundTask]:
        raise NotImplementedError("background_tasks not implemented for Processor")

    async def teardown(self):
        raise NotImplementedError("teardown not implemented for Processor")


T = TypeVar("T", bound=ProcessorAPI)


class ProcessorGroup(Generic[T]):
    group_type: ProcessorType

    def __init__(self, group_id: GroupID, processors: List[T]) -> None:
        self.group_id = group_id
        self.processors = processors
