import os
import json
from pathlib import Path
import tempfile
import unittest

import pyarrow.csv as pcsv
import pyarrow.parquet as pq
import pytest

from buildflow.io.providers.file_provider import FileProvider


@pytest.mark.usefixtures("event_loop_instance")
class FileProviderTest(unittest.TestCase):
    def get_async_result(self, coro):
        """Run a coroutine synchronously."""
        return self.event_loop.run_until_complete(coro)

    def setUp(self) -> None:
        self.output_path = tempfile.mkdtemp()

    def test_push_csv(self):
        path = os.path.join(self.output_path, "output.csv")
        provider = FileProvider(file_path=path, file_format="csv")
        self.get_async_result(provider.push([{"field": 1}, {"field": 2}]))

        table = pcsv.read_csv(Path(path))
        self.assertEqual([{"field": 1}, {"field": 2}], table.to_pylist())

    def test_push_json(self):
        path = os.path.join(self.output_path, "output.json")
        provider = FileProvider(file_path=path, file_format="json")
        self.get_async_result(provider.push([{"field": 1}, {"field": 2}]))

        with open(Path(path), "r") as read_file:
            data = json.load(read_file)

        self.assertEqual([{"field": 1}, {"field": 2}], data)

    def test_push_parquet(self):
        path = os.path.join(self.output_path, "output.parquet")
        provider = FileProvider(file_path=path, file_format="parquet")
        self.get_async_result(provider.push([{"field": 1}, {"field": 2}]))

        table = pq.read_table(path)
        self.assertEqual([{"field": 1}, {"field": 2}], table.to_pylist())

    def test_bad_format(self):
        with self.assertRaises(ValueError):
            FileProvider(file_path="", file_format="bad_format")


if __name__ == "__main__":
    unittest.main()
