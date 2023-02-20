import dataclasses
from enum import Enum
from typing import Any, Dict, List, Optional, TypeVar


class InputOutput:
    """Super class for all input and output types."""
    _io_type: str

    @classmethod
    def from_config(cls, node_info: Dict[str, Any]):
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
    _io_type: str = IOType.Pubsub.value


@dataclasses.dataclass
class BigQuery(InputOutput):

    @dataclasses.dataclass
    class Query:
        # The query to execute.
        query: str
        # Where to temporarily store the results of the query. This is required
        # so we can better parallelize reading in the data.
        # This should be of the format project.temporary_dataset
        temporary_dataset: str

    project: str = ''
    dataset: str = ''
    table: str = ''
    query: Optional[Query] = None
    batch_size: int = 100
    _io_type: str = IOType.BigQuery.value


@dataclasses.dataclass
class RedisStream(InputOutput):
    host: str
    port: str
    streams: List[str]
    stream_positions: Dict[str, str] = dataclasses.field(default_factory=dict)
    _io_type: str = IOType.RedisStream.value


@dataclasses.dataclass
class DuckDB(InputOutput):
    database: str
    table: str = ''
    query: str = ''
    _io_type: str = IOType.DuckDB.value


@dataclasses.dataclass
class Empty(InputOutput):
    inputs: List[Any] = dataclasses.field(default_factory=list)
    _io_type: str = IOType.Empty.value


_IO_MAPPING = {
    IOType.BigQuery.value: BigQuery,
    IOType.Pubsub.value: PubSub,
    IOType.RedisStream.value: RedisStream,
    IOType.DuckDB.value: DuckDB,
    IOType.Empty.value: Empty,
}
