import dataclasses
from typing import Any, Dict, TypeVar


class InputOutput:
    """Super class for all input and output types."""
    _io_type: str

    @classmethod
    def from_config(cls, node_info: Dict[str, Any]):
        io_type = node_info['_io_type']
        return _IO_MAPPING[io_type](**node_info)


IO = TypeVar('IO', bound=InputOutput)


@dataclasses.dataclass
class PubSub(InputOutput):
    topic: str = ''
    subscription: str = ''
    _io_type: str = 'PUBSUB'


@dataclasses.dataclass
class BigQuery(InputOutput):
    project: str = ''
    dataset: str = ''
    table: str = ''
    query: str = ''
    _io_type: str = 'BIG_QUERY'


_IO_MAPPING = {
    'BIG_QUERY': BigQuery,
    'PUBSUB': PubSub,
}
