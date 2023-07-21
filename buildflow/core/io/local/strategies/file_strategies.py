import json
import os
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Type

import fastparquet
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pcsv

from buildflow.core.credentials import EmptyCredentials
from buildflow.core.io.utils.schemas import converters
from buildflow.core.strategies.sink import SinkStrategy
from buildflow.core.types.local_types import FilePath, FileFormat


class FileSink(SinkStrategy):
    def __init__(
        self,
        *,
        credentials: EmptyCredentials,
        file_path: FilePath,
        file_format: FileFormat,
    ):
        super().__init__(credentials=credentials, strategy_id="local-file-sink")
        self.file_path = file_path
        self.file_format = file_format

    def push_converter(
        self, user_defined_type: Type
    ) -> Callable[[Any], Dict[str, Any]]:
        return converters.json_push_converter(user_defined_type)

    async def push(self, batch: Iterable[Dict[str, Any]]):
        if self.file_format == FileFormat.PARQUET:
            exists = os.path.exists(self.file_path)
            fastparquet.write(
                self.file_path, pd.DataFrame.from_records(batch), append=exists
            )
        elif self.file_format == FileFormat.CSV:
            if batch:
                table = pa.Table.from_pylist(batch)
                if (
                    Path(self.file_path).exists()
                    and os.path.getsize(self.file_path) > 0
                ):
                    table = pa.concat_tables([table, pcsv.read_csv(self.file_path)])
                pcsv.write_csv(table, self.file_path)
        elif self.file_format == FileFormat.JSON:
            if batch:
                with open(self.file_path, "a") as output_file:
                    json.dump(batch, output_file)
