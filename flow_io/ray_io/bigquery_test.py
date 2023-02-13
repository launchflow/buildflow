"""Tests for redis.py"""

from dataclasses import dataclass
import json
import os
import shutil
import tempfile
from typing import Any, Dict
import unittest

import ray

from flow_io import ray_io


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


class BigQueryTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=1, num_gpus=0)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def setUp(self) -> None:
        _, self.temp_file = tempfile.mkstemp()
        self.temp_dir = tempfile.mkdtemp()
        self.flow_file = os.path.join(self.temp_dir, 'flow_state.json')
        self.deployment_file = os.path.join(self.temp_dir, 'deployment.json')
        os.environ['FLOW_FILE'] = self.flow_file
        os.environ['FLOW_DEPLOYMENT_FILE'] = self.deployment_file
        with open(self.flow_file, 'w', encoding='UTF-8') as f:
            flow_contents = {
                'nodes': [
                    {
                        'nodeSpace': 'flow_io/ray_io'
                    },
                    {
                        'nodeSpace': 'resource/storage/bigquery/table/table1',
                    },
                    {
                        'nodeSpace': 'resource/storage/bigquery/table/table2',
                    },
                ],
                'outgoingEdges': {
                    'resource/storage/bigquery/table/table1':
                    ['flow_io/ray_io'],
                    'flow_io/ray_io':
                    ['resource/storage/bigquery/table/table2'],
                },
            }
            json.dump(flow_contents, f)
        with open(self.deployment_file, 'w', encoding='UTF-8') as f:
            json.dump(
                {
                    'nodeDeployments': {
                        'resource/storage/bigquery/table/table1': {
                            'project': 'in',
                            'dataset': 'put',
                            'table': 'table',
                        },
                        'resource/storage/bigquery/table/table2': {
                            'project': 'out',
                            'dataset': 'put',
                            'table': 'table',
                        }
                    }
                }, f)

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_end_to_end(self):

        bq_client = FakeBigQueryClient(
            self.temp_file, {'SELECT * FROM `table`': [{
                'field': 1
            }]})

        expected = [{'field': 1}]

        sink = ray_io.sink(bigquery_client=bq_client)
        source = ray_io.source(sink.write,
                               query='SELECT * FROM `table`',
                               bigquery_client=bq_client)
        ray.get(source.run.remote())

        bq_client.load_file()
        got = bq_client.bigquery_data['out.put.table']
        self.assertEqual(expected, got)

    def test_end_to_end_multi_output(self):

        bq_client = FakeBigQueryClient(
            self.temp_file, {'SELECT * FROM `in.put.table`': [{
                'field': 1
            }]})

        expected = [{'field': 1}, {'field': 1}]

        @ray.remote
        class ProcessActor:

            def __init__(self, sink):
                self.sink = sink

            def process(self, elem, carrier):
                return ray.get(sink.write.remote([elem, elem], carrier))

        sink = ray_io.sink(bigquery_client=bq_client)
        processor = ProcessActor.remote(sink)
        source = ray_io.source(processor.process, bigquery_client=bq_client)
        ray.get(source.run.remote())

        bq_client.load_file()
        got = bq_client.bigquery_data['out.put.table']
        self.assertEqual(expected, got)


if __name__ == '__main__':
    unittest.main()
