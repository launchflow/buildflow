from enum import Enum
import os
import json
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Type, Union

import fastparquet
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pcsv

from buildflow.io.providers import PushProvider
from buildflow.io.providers.schemas import converters


class FileFormat(Enum):
    # TODO: Support additional file formats (Arrow, Avro, etc..)
    PARQUET = 1
    CSV = 2
    JSON = 3


class FileProvider(PushProvider):
    def __init__(
        self,
        file_path: str,
        file_format: Union[str, FileFormat],
        should_repeat: bool = True,
    ):
        """
        Creates a PulsingProvider that will emit the given items at the given interval.

        Args:
            items: The items to emit.
            pulse_interval_seconds: The interval between each item being emitted.
            should_repeat: Whether to repeat the items after they have all been emitted.
                Defaults to True.
        """
        super().__init__()
        self.file_path = file_path
        self.file_format = file_format
        if isinstance(file_format, str):
            try:
                self._format = FileFormat[file_format.upper()]
            except KeyError:
                raise ValueError(f"Invalid file format: `{file_format}`")
        self.should_repeat = should_repeat
        self._to_emit = 0

    def push_converter(
        self, user_defined_type: Type
    ) -> Callable[[Any], Dict[str, Any]]:
        return converters.json_push_converter(user_defined_type)

    async def push(self, batch: Iterable[Dict[str, Any]]):
        if self._format == FileFormat.PARQUET:
            exists = os.path.exists(self.file_path)
            fastparquet.write(
                self.file_path, pd.DataFrame.from_records(batch), append=exists
            )
        elif self._format == FileFormat.CSV:
            if batch:
                table = pa.Table.from_pylist(batch)
                if (
                    Path(self.file_path).exists()
                    and os.path.getsize(self.file_path) > 0
                ):
                    table = pa.concat_tables([table, pcsv.read_csv(self.file_path)])
                pcsv.write_csv(table, self.file_path)
        elif self._format == FileFormat.JSON:
            if batch:
                with open(self.file_path, "a") as output_file:
                    json.dump(batch, output_file)
