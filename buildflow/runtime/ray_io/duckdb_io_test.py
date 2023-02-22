"""Tests for DuckDB Ray IO Connectors."""

import unittest

import pytest
import ray


@pytest.mark.skip('DuckDB is not fully supported yet')
class DuckDBTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=1, num_gpus=0)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_end_to_end(self):
        pass

    def test_end_to_end_multi_output(self):
        pass


if __name__ == '__main__':
    unittest.main()
