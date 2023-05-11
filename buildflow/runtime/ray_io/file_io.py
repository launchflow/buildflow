"""IO for writing to local files."""

from dataclasses import dataclass
from enum import Enum
import os
import json
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Union

import fastparquet
import pandas as pd
import ray
import pyarrow as pa
import pyarrow.csv as pcsv

from buildflow.api import io
from buildflow.runtime.ray_io import base


class FileFormat(Enum):
    # TODO: Support additional file formats (CSV, JSON, Arrow, Avro, etc..)
    PARQUET = 1
    CSV = 2
    JSON = 3


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
        """Based on the instance of `elements` param, write the values
        to the supported file types.

        For Ray Dataset, file path becomes a folder and individual
        dictionary become individual file.
        For Iterable type, file path results in a single file of
        the `FileFormat` type.

        :param elements: Data
        :type elements: Union[ray.data.Dataset, Iterable[Dict[str, Any]]]
        """
        if isinstance(elements, ray.data.Dataset):
            if self._format == FileFormat.PARQUET:
                elements.write_parquet(self._path)
            elif self._format == FileFormat.CSV:
                elements.write_csv(self._path)
            elif self._format == FileFormat.JSON:
                elements.write_json(self._path)
        else:
            if self._format == FileFormat.PARQUET:
                exists = os.path.exists(self._path)
                fastparquet.write(self._path,
                                  pd.DataFrame.from_records(elements),
                                  append=exists)
            elif self._format == FileFormat.CSV:
                if elements:
                    table = pa.Table.from_pylist(elements)
                    if Path(self._path).exists():
                        table = pa.concat_tables(
                            [table, pcsv.read_csv(self._path)]
                        )
                    pcsv.write_csv(table, self._path)
            elif self._format == FileFormat.JSON:
                if elements:
                    with open(self._path, 'a') as output_file:
                        json.dump(elements, output_file)
