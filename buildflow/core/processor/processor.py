import enum
from typing import Any, Dict, List

from buildflow.core.background_tasks.background_task import BackgroundTask
from buildflow.core.resources._resource import Resource


class ProcessorType(enum.Enum):
    COLLECTOR = "collector"
    CONNECTION = "connection"
    PIPELINE = "pipeline"
    SERVICE = "service"


ProcessorID = str


class ProcessorAPI:
    processor_id: ProcessorID
    processor_type: ProcessorType
    __meta__: Dict[str, Any]

    def resources(self) -> List[Resource]:
        raise NotImplementedError("resources not implemented for Processor")

    def setup(self):
        raise NotImplementedError("setup not implemented")

    def process(self, *args, **kwargs):
        raise NotImplementedError("process not implemented for Processor")

    def background_tasks(self) -> List[BackgroundTask]:
        raise NotImplementedError("background_tasks not implemented for Pipeline")

    async def teardown(self):
        raise NotImplementedError("teardown not implemented for Processor")
