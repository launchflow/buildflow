"""Tests for redis.py"""

from dataclasses import dataclass
import json
import tempfile
import time
from typing import Any, Dict
import unittest

import ray
from ray.dag.input_node import InputNode

from flow_io.ray_io import bigquery


@dataclass
class FakeQueryJob:
    rows: Dict[str, Any]

    def result(self):
        return self.rows


class FakeBigQueryClient:

    def __init__(self,
                 temp_file: str,
                 bigquery_data: Dict[str, Any] = {}) -> None:
        self.temp_file = temp_file
        self.bigquery_data = bigquery_data
        self._write_file()

    def _write_file(self):
        with open(self.temp_file, 'w') as f:
            json.dump(self.bigquery_data, f)

    def load_file(self):
        with open(self.temp_file, 'r') as f:
            self.bigquery_data = json.load(f)

    def query(self, query: str):
        self.load_file()
        return FakeQueryJob(self.bigquery_data[query])

    def insert_rows(self, bigquery_table: str, rows):
        self.load_file()
        self.bigquery_data[bigquery_table] = rows
        self._write_file()


class RedisStream(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=1, num_gpus=0)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def setUp(self) -> None:
        _, self.temp_file = tempfile.mkstemp()

    def test_end_to_end(self):

        @ray.remote
        def ray_func(input):
            return input

        bq_client = FakeBigQueryClient(
            self.temp_file, {'SELECT * FROM `table`': [{
                'field': 1
            }]})

        input_config = {
            'query': 'SELECT * FROM `table`',
            'project': '',
            'dataset': '',
            'table': '',
        }

        output_config = {
            'project': 'out',
            'dataset': 'put',
            'table': 'table',
            'query': '',
        }

        expected = [{'field': 1}]

        with InputNode() as input:
            ray_func_ref = ray_func.bind(input)
            output_ref = bigquery.BigQueryOutput.bind(
                **output_config, bigquery_client=bq_client)
            output = output_ref.write.bind(ray_func_ref)

        bigquery.BigQueryInput([output],
                               **input_config,
                               bigquery_client=bq_client).run()

        time.sleep(10)
        bq_client.load_file()
        got = bq_client.bigquery_data['out.put.table']
        self.assertEqual(expected, got)


if __name__ == '__main__':
    unittest.main()
