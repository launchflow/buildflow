import dataclasses
from typing import List, Optional, Set, Union

from buildflow.core.processor.processor import ProcessorGroupType, ProcessorType
from buildflow.io.endpoint import Method, Route
from buildflow.io.primitive import Primitive, PrimitiveType


def _find_parent_primitives(
    primitive: Primitive,
    primitive_state: "PrimitiveState",
    managed_primitives: Set[str],
    visited_primitives: Set[str],
) -> List["PrimitiveState"]:
    fields = dataclasses.fields(primitive)
    parent_primitives = []
    for field in fields:
        field_value = getattr(primitive, field.name)
        if field_value is not None and isinstance(field_value, Primitive):
            primitive_state.primitive_dependencies.append(field_value.primitive_id())
            if field_value.primitive_id() not in visited_primitives:
                visited_primitives.add(field_value.primitive_id())
                parent_primitives.extend(
                    PrimitiveState.from_primitive(
                        primitive=field_value,
                        managed_primitives=managed_primitives,
                        visited_primitives=visited_primitives,
                    )
                )
    return parent_primitives


@dataclasses.dataclass
class PrimitiveState:
    primitive_id: str
    primitive_name: str
    primitive_type: PrimitiveType
    managed: bool
    primitive_dependencies: List[str] = dataclasses.field(default_factory=list)
    resource_url: Optional[str] = None

    def to_dict(self):
        return {
            "primitive_id": self.primitive_id,
            "primitive_name": self.primitive_name,
            "primitive_type": self.primitive_type,
            "managed": self.managed,
            "resource_url": self.resource_url,
            "primitive_dependencies": self.primitive_dependencies,
        }

    @classmethod
    def from_primitive(
        cls,
        primitive: Primitive,
        managed_primitives: Set[str],
        visited_primitives: Set[str],
    ) -> List["PrimitiveState"]:
        ps = PrimitiveState(
            primitive_id=primitive.primitive_id(),
            primitive_name=type(primitive).__name__,
            primitive_type=primitive.primitive_type.value,
            managed=primitive.primitive_id() in managed_primitives,
            resource_url=primitive.cloud_console_url(),
        )
        additional = _find_parent_primitives(
            primitive, ps, managed_primitives, visited_primitives
        )
        additional.append(ps)
        return additional


@dataclasses.dataclass
class ConsumerState:
    source_id: str
    sink_id: Optional[str]

    def to_dict(self):
        return {"source_id": self.source_id, "sink_id": self.sink_id}


@dataclasses.dataclass
class CollectorState:
    sink_id: Optional[str]

    def to_dict(self):
        return {"sink_id": self.sink_id}


@dataclasses.dataclass
class EndpointState:
    route: Route
    method: Method

    def to_dict(self):
        return {"route": self.route, "method": self.method.value}


@dataclasses.dataclass
class ProcessorState:
    processor_id: str
    processor_type: ProcessorType
    processor_info: Union[ConsumerState, CollectorState, EndpointState]
    primitive_dependencies: List[str]

    def to_dict(self):
        return {
            "processor_id": self.processor_id,
            "processor_type": self.processor_type.value,
            "processor_info": self.processor_info.to_dict(),
            "primitive_dependencies": self.primitive_dependencies,
        }


@dataclasses.dataclass
class ServiceState:
    base_route: str

    def to_dict(self):
        return {"base_route": self.base_route}


@dataclasses.dataclass
class ConsumerGroupState:
    def to_dict(self):
        return {}


@dataclasses.dataclass
class CollectorGroupState:
    base_route: str

    def to_dict(self):
        return {"base_route": self.base_route}


@dataclasses.dataclass
class ProcessorGroupState:
    processor_group_id: str
    processor_group_type: ProcessorGroupType
    processor_states: List[ProcessorState]
    group_info: Union[ConsumerGroupState, CollectorGroupState, ServiceState]

    def to_dict(self):
        return {
            "processor_group_id": self.processor_group_id,
            "processor_group_type": self.processor_group_type.value,
            "processor_states": [p.to_dict() for p in self.processor_states],
            "group_info": self.group_info.to_dict(),
        }


@dataclasses.dataclass
class FlowState:
    flow_id: str
    stack: str
    primitive_states: List[PrimitiveState]
    processor_group_states: List[ProcessorGroupState]
    python_version: str
    ray_version: str
    buildflow_version: str

    def to_dict(self):
        return {
            "flow_id": self.flow_id,
            "stack": self.stack,
            "primitive_states": [p.to_dict() for p in self.primitive_states],
            "processor_group_states": [
                p.to_dict() for p in self.processor_group_states
            ],
            "python_version": self.python_version,
            "ray_version": self.ray_version,
            "buildflow_version": self.buildflow_version,
        }
