from enum import Enum
import dataclasses
from typing import Any, Dict, List, TypeVar


class InputOutput:
    """Super class for all input and output types."""
    _io_type: str

    @classmethod
    def from_config(cls, node_info: Dict[str, Any]):
        print('DO NOT SUBMIT: ', node_info)
        io_type = node_info['_io_type']
        return _IO_MAPPING[io_type](**node_info)


IO = TypeVar('IO', bound=InputOutput)


class IOType(Enum):
    Pubsub = 'PUBSUB'
    BigQuery = 'BIG_QUERY'
    RedisStream = 'REDIS_STREAM'
    DuckDB = 'DUCKDB'
    Empty = 'EMPTY'


@dataclasses.dataclass
class PubSub(InputOutput):
    topic: str = ''
    subscription: str = ''
    _io_type: str = IOType.Pubsub.name


@dataclasses.dataclass
class BigQuery(InputOutput):
    project: str = ''
    dataset: str = ''
    table: str = ''
    query: str = ''
    _io_type: str = IOType.BigQuery.name


@dataclasses.dataclass
class RedisStream(InputOutput):
    host: str
    port: str
    streams: List[str]
    stream_positions: Dict[str, str] = dataclasses.field(default_factory=dict)
    _io_type: str = IOType.RedisStream.name


@dataclasses.dataclass
class DuckDB(InputOutput):
    database: str
    table: str = ''
    query: str = IOType.DuckDB.name
    _io_type = IOType.Empty.name


@dataclasses.dataclass
class Empty(InputOutput):
    inputs: List[Any] = dataclasses.field(default_factory=list)
    _io_type: str = IOType.Empty.name


_IO_MAPPING = {
    IOType.BigQuery.name: BigQuery,
    IOType.Pubsub.name: PubSub,
    IOType.RedisStream.name: RedisStream,
    IOType.DuckDB.name: DuckDB,
    IOType.Empty.name: Empty,
}
