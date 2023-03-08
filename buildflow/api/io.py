import dataclasses
from typing import Any, Dict, List, TypeVar


class InputOutput:
    """Super class for all input and output types."""
    pass


IO = TypeVar('IO', bound=InputOutput)


@dataclasses.dataclass(frozen=True)
class HTTPEndpoint(InputOutput):
    host: str = 'localhost'
    port: int = 3569


@dataclasses.dataclass(frozen=True)
class PubSub(InputOutput):
    topic: str = ''
    subscription: str = ''


@dataclasses.dataclass(frozen=True)
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


@dataclasses.dataclass(frozen=True)
class RedisStream(InputOutput):
    host: str
    port: str
    streams: List[str]
    start_positions: Dict[str, str] = dataclasses.field(default_factory=dict)
    # Read timeout. If > 0 this is how long we will read from the redis stream.
    read_timeout_secs: int = -1


@dataclasses.dataclass(frozen=True)
class DuckDB(InputOutput):
    database: str
    table: str = ''
    query: str = ''


@dataclasses.dataclass(frozen=True)
class Empty(InputOutput):
    inputs: List[Any] = dataclasses.field(default_factory=list)
