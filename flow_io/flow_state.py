import dataclasses

from typing import List, TypeVar


class InputOutput:
    """Super class for all input and output types."""


IO = TypeVar('IO', bound=InputOutput)


@dataclasses.dataclass
class NodeState:
    entrypoint_file: str
    inputs: List[IO]
    output: List[IO]


@dataclasses.dataclass
class FlowState:
    node_states: List[NodeState]
