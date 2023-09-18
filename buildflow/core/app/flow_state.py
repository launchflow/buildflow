import dataclasses
from typing import List, Optional, Union

from buildflow.core.processor.processor import ProcessorGroupType, ProcessorType
from buildflow.io.endpoint import Method, Route


@dataclasses.dataclass
class PrimitiveState:
    primitive_id: str
    primitive_name: str
    managed: bool
    resource_url: Optional[str] = None


@dataclasses.dataclass
class ConsumerState:
    source_id: str
    sink_id: Optional[str]


@dataclasses.dataclass
class CollectorState:
    sink_id: Optional[str]


@dataclasses.dataclass
class EndpointState:
    route: Route
    method: Method


@dataclasses.dataclass
class ProcessorState:
    processor_id: str
    processor_type: ProcessorType
    processor_info: Union[ConsumerState, CollectorState, EndpointState]
    primitive_dependencies: List[str]


@dataclasses.dataclass
class ProcessorGroupState:
    processor_group_id: str
    processor_group_type: ProcessorGroupType
    processor_states: List[ProcessorState]


@dataclasses.dataclass
class FlowState:
    flow_id: str
    primitive_states: List[PrimitiveState]
    processor_group_states: List[ProcessorGroupState]
