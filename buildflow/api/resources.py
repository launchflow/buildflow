import dataclasses
from enum import Enum
from typing import Any, Dict, List, TypeVar


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

    # The BigQuery table to read from.
    # Should be of the format project.dataset.table
    table_id: str = ''
    # The query to read data from.
    query: str = ''
    # The temporary dataset to store query results in. If unspecified we will
    # attempt to create one.
    temp_dataset: str = ''
    # The billing project to use for query usage. If unset we will use the
    # project configured with application default credentials.
    billing_project: str = ''
    # The temporary gcs bucket uri to store temp data in. If unspecified we
    # will attempt to create one.
    temp_gcs_bucket: str = ''

    batch_size: int = 1000
    _io_type: str = IOType.BigQuery.value


@dataclasses.dataclass
class RedisStream(InputOutput):
    host: str
    port: str
    streams: List[str]
    start_positions: Dict[str, str] = dataclasses.field(default_factory=dict)
    # Read timeout. If > 0 this is how long we will read from the redis stream.
    read_timeout_secs: int = -1
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
