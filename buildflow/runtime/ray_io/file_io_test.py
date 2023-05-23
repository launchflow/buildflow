import os
import json
import shutil
import tempfile
import unittest
from pathlib import Path

import pyarrow.csv as pcsv
import pyarrow.parquet as pq
import ray

import buildflow


class FileIoTest(unittest.TestCase):
    def setUp(self) -> None:
        self.output_path = tempfile.mkdtemp()
        self.flow = buildflow.Node()

    def tearDown(self) -> None:
        shutil.rmtree(self.output_path)

    def test_write_dictionaries(self):
        path = os.path.join(self.output_path, "output.parquet")

        @self.flow.processor(
            source=buildflow.EmptySource(
                inputs=[
                    {
                        "field": 1,
                    },
                    {"field": 2},
                ]
            ),
            sink=buildflow.FileSink(
                file_path=path, file_format=buildflow.FileFormat.PARQUET
            ),
        )
        def process(elem):
            return elem

        self.flow.run().output()
        table = pq.read_table(path)
        self.assertEqual([{"field": 1}, {"field": 2}], table.to_pylist())

    def test_write_dataset(self):
        path = os.path.join(self.output_path, "output.parquet")

        @self.flow.processor(
            source=buildflow.EmptySource(
                inputs=ray.data.from_items(
                    [
                        {
                            "field": 1,
                        },
                        {"field": 2},
                    ]
                )
            ),
            sink=buildflow.FileSink(
                file_path=path, file_format=buildflow.FileFormat.PARQUET
            ),
        )
        def process(elem):
            return elem

        self.flow.run().output()
        table = pq.read_table(path)
        self.assertEqual([{"field": 1}, {"field": 2}], table.to_pylist())

    def test_write_csv_from_dictionaries(self):
        path = os.path.join(self.output_path, "output.csv")

        @self.flow.processor(
            source=buildflow.EmptySource(
                inputs=[
                    {
                        "field": 1,
                    },
                    {"field": 2},
                ]
            ),
            sink=buildflow.FileSink(
                file_path=path, file_format=buildflow.FileFormat.CSV
            ),
        )
        def process(elem):
            return elem

        self.flow.run().output()

        # read csv file
        table = pcsv.read_csv(Path(path))
        self.assertEqual([{"field": 1}, {"field": 2}], table.to_pylist())

    def test_write_csv_from_ray_datasets(self):
        path = os.path.join(self.output_path, "output.csv")

        @self.flow.processor(
            source=buildflow.EmptySource(
                inputs=ray.data.from_items(
                    [
                        {
                            "field": 1,
                        },
                        {"field": 2},
                    ]
                )
            ),
            sink=buildflow.FileSink(
                file_path=path, file_format=buildflow.FileFormat.CSV
            ),
        )
        def process(elem):
            return elem

        self.flow.run().output()

        # read all csvs in the folder
        ray_dataset = ray.data.read_csv(path)
        self.assertEqual([{"field": 1}, {"field": 2}], ray_dataset.take_all())

    def test_write_json_from_dictionaries(self):
        path = os.path.join(self.output_path, "output.json")

        @self.flow.processor(
            source=buildflow.EmptySource(
                inputs=[
                    {
                        "field": 1,
                    },
                    {"field": 2},
                ]
            ),
            sink=buildflow.FileSink(
                file_path=path, file_format=buildflow.FileFormat.JSON
            ),
        )
        def process(elem):
            return elem

        self.flow.run().output()

        # read json file
        with open(Path(path), "r") as read_file:
            data = json.load(read_file)

        self.assertEqual([{"field": 1}, {"field": 2}], data)

    def test_write_json_from_ray_datasets(self):
        path = os.path.join(self.output_path, "output.json")

        @self.flow.processor(
            source=buildflow.EmptySource(
                inputs=ray.data.from_items(
                    [
                        {
                            "field": 1,
                        },
                        {"field": 2},
                    ]
                )
            ),
            sink=buildflow.FileSink(
                file_path=path, file_format=buildflow.FileFormat.JSON
            ),
        )
        def process(elem):
            return elem

        self.flow.run().output()

        # read all jsons in the folder
        ray_dataset = ray.data.read_json(path)
        self.assertEqual([{"field": 1}, {"field": 2}], ray_dataset.take_all())


if __name__ == "__main__":
    unittest.main()
