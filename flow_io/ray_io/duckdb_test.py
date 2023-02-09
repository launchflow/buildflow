"""Tests for DuckDB Ray IO Connectors."""

import json
import os
import shutil
import tempfile
import unittest

import duckdb
import ray

from flow_io import ray_io


class DuckDBTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=1, num_gpus=0)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.flow_file = os.path.join(self.temp_dir, 'flow_state.json')
        self.deployment_file = os.path.join(self.temp_dir, 'deployment.json')
        os.environ['FLOW_FILE'] = self.flow_file
        os.environ['FLOW_DEPLOYMENT_FILE'] = self.deployment_file
        self.input_database = os.path.join(self.temp_dir, 'input.duckdb')
        self.output_database = os.path.join(self.temp_dir, 'output.duckdb')
        with open(self.flow_file, 'w', encoding='UTF-8') as f:
            flow_contents = {
                'nodes': [
                    {
                        'nodeSpace': 'flow_io/ray_io'
                    },
                    {
                        'nodeSpace': 'resource/storage/duckdb/table/table1',
                    },
                    {
                        'nodeSpace': 'resource/storage/duckdb/table/table2',
                    },
                ],
                'outgoingEdges': {
                    'resource/storage/duckdb/table/table1': ['flow_io/ray_io'],
                    'flow_io/ray_io': ['resource/storage/duckdb/table/table2'],
                },
            }
            json.dump(flow_contents, f)
        with open(self.deployment_file, 'w', encoding='UTF-8') as f:
            json.dump(
                {
                    'nodeDeployments': {
                        'resource/storage/duckdb/table/table1': {
                            'database': self.input_database,
                            'table': 'input_table',
                        },
                        'resource/storage/duckdb/table/table2': {
                            'database': self.output_database,
                            'table': 'output_table',
                        }
                    }
                }, f)

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_dir)

    def test_end_to_end(self):
        input_con = duckdb.connect(self.input_database)
        input_con.execute('CREATE TABLE input_table(number INTEGER)')
        input_con.execute('INSERT INTO input_table VALUES (1), (2), (3)')
        input_con.close()

        sink = ray_io.sink()
        source = ray_io.source(sink.write)
        ray.get(source.run.remote())

        output_con = duckdb.connect(self.output_database)
        output_con.execute('SELECT * FROM output_table')
        rows = output_con.fetchall()

        expected = [(1, ), (2, ), (3, )]

        self.assertEqual(expected, rows)

    def test_end_to_end_multi_output(self):

        input_con = duckdb.connect(self.input_database)
        input_con.execute('CREATE TABLE input_table(number INTEGER)')
        input_con.execute('INSERT INTO input_table VALUES (1), (2), (3)')
        input_con.close()

        @ray.remote
        class ProcessActor:

            def __init__(self, sink):
                self.sink = sink

            def process(self, elem):
                return ray.get(sink.write.remote([elem, elem]))

        sink = ray_io.sink()
        processor = ProcessActor.remote(sink)
        source = ray_io.source(processor.process)
        ray.get(source.run.remote())

        output_con = duckdb.connect(self.output_database)
        output_con.execute('SELECT * FROM output_table')
        rows = output_con.fetchall()

        expected = [(1, ), (1, ), (2, ), (2, ), (3, ), (3, )]

        self.assertEqual(expected, rows)


if __name__ == '__main__':
    unittest.main()
