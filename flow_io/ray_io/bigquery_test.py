"""Tests for redis.py"""

import json
import os
import tempfile
import unittest
from dataclasses import dataclass
from typing import Any, Dict

import pytest
import ray

import flow_io
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


@pytest.mark.skip(
    reason='TODO: need to update this to do something with bq client.')
class BigQueryTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=1, num_gpus=0)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def setUp(self):
        _, self.temp_file = tempfile.mkstemp()

    def tearDown(self):
        os.remove(self.temp_file)

    def test_end_to_end(self):
        input_row = {'field': 1}
        expected = [input_row]
        fake_bq_client = FakeBigQueryClient(
            self.temp_file, {'SELECT * FROM `table`': [input_row]})
        bq_ref = flow_io.BigQuery(project='project',
                                  dataset='dataset',
                                  table='table',
                                  query='SELECT * FROM `table`')

        @ray.remote
        def pass_through_fn(elem):
            return elem

        sink_actors = [
            ray_io.bigquery.BigQuerySinkActor.remote(pass_through_fn.remote,
                                                     bq_ref, fake_bq_client)
        ]
        source_actor = ray_io.bigquery.BigQuerySourceActor.remote(
            sink_actors, bq_ref, fake_bq_client)
        ray.get(source_actor.run.remote())

        fake_bq_client.load_file()
        got = fake_bq_client.bigquery_data['project.dataset.table']
        self.assertEqual(expected, got)

    def test_end_to_end_multi_output(self):
        input_row = {'field': 1}
        expected = [input_row, input_row]
        fake_bq_client = FakeBigQueryClient(
            self.temp_file, {'SELECT * FROM `table`': [input_row]})
        bq_ref = flow_io.BigQuery(project='project',
                                  dataset='dataset',
                                  table='table',
                                  query='SELECT * FROM `table`')

        @ray.remote
        class ProcessActor:

            def process(self, elem):
                return [elem, elem]

        processor = ProcessActor.remote()
        sink_actors = [
            ray_io.bigquery.BigQuerySinkActor.remote(processor.process.remote,
                                                     bq_ref, fake_bq_client)
        ]
        source_actor = ray_io.bigquery.BigQuerySourceActor.remote(
            sink_actors, bq_ref, fake_bq_client)
        ray.get(source_actor.run.remote())

        fake_bq_client.load_file()
        got = fake_bq_client.bigquery_data['project.dataset.table']
        self.assertEqual(expected, got)


if __name__ == '__main__':
    unittest.main()
