import enum
from typing import List

from buildflow.core.background_tasks.background_task import BackgroundTask


class ProcessorType(enum.Enum):
    COLLECTOR = "collector"
    CONNECTION = "connection"
    PIPELINE = "pipeline"
    ENDPOINT = "endpoint"


ProcessorID = str


class ProcessorAPI:
    processor_id: ProcessorID
    processor_type: ProcessorType

    def pulumi_program(self, preview: bool):
        raise NotImplementedError("pulumi_program not implemented for Processor")

    def setup(self):
        raise NotImplementedError("setup not implemented")

    async def process(self, *args, **kwargs):
        raise NotImplementedError("process not implemented for Processor")

    def background_tasks(self) -> List[BackgroundTask]:
        raise NotImplementedError("background_tasks not implemented for Pipeline")

    async def teardown(self):
        raise NotImplementedError("teardown not implemented for Processor")
