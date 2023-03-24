import os
import shutil
import tempfile
import unittest

import pyarrow.parquet as pq
import ray

import buildflow


class FileIoTest(unittest.TestCase):

    def setUp(self) -> None:
        self.output_path = tempfile.mkdtemp()
        self.flow = buildflow.Flow()

    def tearDown(self) -> None:
        shutil.rmtree(self.output_path)

    def test_write_dictionaries(self):

        path = os.path.join(self.output_path, 'output.parquet')

        @self.flow.processor(
            source=buildflow.EmptySource(inputs=[{
                'field': 1,
            }, {
                'field': 2
            }]),
            sink=buildflow.FileSink(file_path=path,
                                    file_format=buildflow.FileFormat.PARQUET),
        )
        def process(elem):
            return elem

        self.flow.run().output()
        table = pq.read_table(path)
        self.assertEqual([{'field': 1}, {'field': 2}], table.to_pylist())

    def test_write_dataset(self):

        path = os.path.join(self.output_path, 'output.parquet')

        @self.flow.processor(
            source=buildflow.EmptySource(
                inputs=ray.data.from_items([{
                    'field': 1,
                }, {
                    'field': 2
                }])),
            sink=buildflow.FileSink(file_path=path,
                                    file_format=buildflow.FileFormat.PARQUET),
        )
        def process(elem):
            return elem

        self.flow.run().output()
        table = pq.read_table(path)
        self.assertEqual([{'field': 1}, {'field': 2}], table.to_pylist())


if __name__ == '__main__':
    unittest.main()
