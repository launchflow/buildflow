import json
from typing import Any, Callable, Dict, Iterable, Type

import fastparquet
import fsspec
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pcsv
from fsspec.implementations.local import LocalFileSystem

from buildflow.core.credentials import EmptyCredentials
from buildflow.core.types.shared_types import FilePath
from buildflow.core.utils import uuid
from buildflow.io.strategies.sink import SinkStrategy
from buildflow.io.utils.schemas import converters
from buildflow.types.portable import FileFormat


class FileSink(SinkStrategy):
    def __init__(
        self,
        *,
        credentials: EmptyCredentials,
        file_path: FilePath,
        file_format: FileFormat,
        file_system: fsspec.AbstractFileSystem = LocalFileSystem(),
    ):
        super().__init__(credentials=credentials, strategy_id="local-file-sink")
        split_path = file_path.split(".")
        if len(split_path) > 1:
            extension = split_path[-1]
            base_file_path = ".".join(split_path[:-1])
        else:
            base_file_path = file_path
            extension = ""
        # This ensures each replica is writing to a unique file
        self.file_path = f"{base_file_path}-{uuid(8)}.{extension}"
        self.file_format = file_format
        self.file_system = file_system

    def push_converter(
        self, user_defined_type: Type
    ) -> Callable[[Any], Dict[str, Any]]:
        return converters.json_push_converter(user_defined_type)

    async def push(self, batch: Iterable[Dict[str, Any]]):
        exists = self.file_system.exists(self.file_path)
        if self.file_format == FileFormat.PARQUET:
            fastparquet.write(
                self.file_path,
                pd.DataFrame.from_records(batch),
                append=exists,
                open_with=self.file_system.open,
            )
        elif self.file_format == FileFormat.CSV:
            if batch:
                table = pa.Table.from_pylist(batch)
                if exists and self.file_system.size(self.file_path) > 0:
                    with self.file_system.open(self.file_path, "rb") as source:
                        table = pa.concat_tables([table, pcsv.read_csv(source)])
                with self.file_system.open(self.file_path, "wb") as output:
                    pcsv.write_csv(table, output)
        elif self.file_format == FileFormat.JSON:
            if batch:
                with self.file_system.open(self.file_path, "a") as output_file:
                    json.dump(batch, output_file)
        else:
            raise ValueError(f"Unknown file format: {self.file_format}")
