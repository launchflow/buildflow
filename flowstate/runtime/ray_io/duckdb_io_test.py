"""Tests for DuckDB Ray IO Connectors."""

import os
import shutil
import tempfile
import unittest

import duckdb
import ray

import flowstate


class DuckDBTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=1, num_gpus=0)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.input_database = os.path.join(self.temp_dir, 'input.duckdb')
        self.output_database = os.path.join(self.temp_dir, 'output.duckdb')

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_dir)

    def test_end_to_end(self):
        input_con = duckdb.connect(self.input_database)
        input_con.execute('CREATE TABLE input_table(number INTEGER)')
        input_con.execute('INSERT INTO input_table VALUES (1), (2), (3)')
        input_con.close()

        flowstate.init(
            config={
                'input':
                flowstate.DuckDB(database=self.input_database,
                                  table='input_table'),
                'outputs': [
                    flowstate.DuckDB(database=self.output_database,
                                      table='output_table')
                ]
            })

        @ray.remote
        def process(elem):
            return elem

        flowstate.run(process.remote)

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

        flowstate.init(
            config={
                'input':
                flowstate.DuckDB(database=self.input_database,
                                  table='input_table'),
                'outputs': [
                    flowstate.DuckDB(database=self.output_database,
                                      table='output_table')
                ]
            })

        @ray.remote
        def process(elem):
            return [elem, elem]

        flowstate.run(process.remote)

        output_con = duckdb.connect(self.output_database)
        output_con.execute('SELECT * FROM output_table')
        rows = output_con.fetchall()

        expected = [(1, ), (1, ), (2, ), (2, ), (3, ), (3, )]

        self.assertEqual(expected, rows)


if __name__ == '__main__':
    unittest.main()
