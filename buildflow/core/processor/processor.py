from typing import List, Dict, Any

from buildflow.core.resources._resource import Resource
import enum


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
