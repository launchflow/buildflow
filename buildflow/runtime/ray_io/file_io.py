"""IO for writing to local files."""

from dataclasses import dataclass
from enum import Enum
import os
from typing import Any, Callable, Dict, Iterable, Union

import fastparquet
import pandas as pd
import ray

from buildflow.api import io
from buildflow.runtime.ray_io import base


class FileFormat(Enum):
    # TODO: Support additional file formats (CSV, JSON, Arrow, Avro, etc..)
    PARQUET = 1


@dataclass
class FileSink(io.Sink):
    file_path: str
    file_format: FileFormat

    def __post_init__(self):
        # TODO: right now this only supports writing files locally, we should
        # also support writing to GCS / S3 based on the path. gs:// = GCS
        # s3:// = S3
        abs_path = os.path.abspath(self.file_path)
        split_path = os.path.split(abs_path)
        if split_path[0]:
            os.makedirs(os.path.join(split_path[0]), exist_ok=True)

    def actor(self, remote_fn: Callable, is_streaming: bool):
        return FileSinkActor.remote(remote_fn, self)


@ray.remote(num_cpus=FileSink.num_cpus())
class FileSinkActor(base.RaySink):

    def __init__(self, remote_fn, file_sink: FileSink) -> None:
        super().__init__(remote_fn)
        self._format = file_sink.file_format
        # TODO: right now this only supports writing files locally, we should
        # also support writing to GCS / S3 based on the path. gs:// = GCS
        # s3:// = S3
        self._path = file_sink.file_path

    async def _write(self, elements: Union[ray.data.Dataset,
                                           Iterable[Dict[str, Any]]]):
        if isinstance(elements, ray.data.Dataset):
            if self._format == FileFormat.PARQUET:
                elements.write_parquet(self._path)
        else:
            if self._format == FileFormat.PARQUET:
                exists = os.path.exists(self._path)
                fastparquet.write(self._path,
                                  pd.DataFrame.from_records(elements),
                                  append=exists)
