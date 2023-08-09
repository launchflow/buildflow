import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pyarrow.csv as pcsv
import pyarrow.parquet as pq
import pytest

from buildflow.io.local.file import File
from buildflow.types.portable import FileFormat


@pytest.mark.usefixtures("event_loop_instance")
class FileProviderTest(unittest.TestCase):
    def get_output_file(self) -> str:
        files = os.listdir(self.output_path)
        self.assertEqual(1, len(files))
        return os.path.join(self.output_path, files[0])

    def get_async_result(self, coro):
        """Run a coroutine synchronously."""
        return self.event_loop.run_until_complete(coro)

    def setUp(self) -> None:
        self.output_path = tempfile.mkdtemp()

    def test_push_csv(self):
        file_path = os.path.join(self.output_path, "output.csv")
        local_file = File(
            file_path=file_path,
            file_format=FileFormat.CSV,
        )

        file_sink = local_file.sink_provider().sink(mock.MagicMock())
        self.get_async_result(file_sink.push([{"field": 1}, {"field": 2}]))

        file_path = self.get_output_file()
        table = pcsv.read_csv(Path(file_path))
        self.assertEqual([{"field": 1}, {"field": 2}], table.to_pylist())

    def test_push_json(self):
        file_path = os.path.join(self.output_path, "output.json")
        local_file = File(
            file_path=file_path,
            file_format=FileFormat.JSON,
        )

        file_sink = local_file.sink_provider().sink(mock.MagicMock())
        self.get_async_result(file_sink.push([{"field": 1}, {"field": 2}]))

        file_path = self.get_output_file()
        with open(Path(file_path), "r") as read_file:
            data = json.load(read_file)

        self.assertEqual([{"field": 1}, {"field": 2}], data)

    def test_push_parquet(self):
        file_path = os.path.join(self.output_path, "output.parquet")
        local_file = File(
            file_path=file_path,
            file_format=FileFormat.PARQUET,
        )

        file_sink = local_file.sink_provider().sink(mock.MagicMock())
        self.get_async_result(file_sink.push([{"field": 1}, {"field": 2}]))
        file_path = self.get_output_file()
        table = pq.read_table(file_path)
        self.assertEqual([{"field": 1}, {"field": 2}], table.to_pylist())


if __name__ == "__main__":
    unittest.main()
