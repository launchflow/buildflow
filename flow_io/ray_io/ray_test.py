"""Rays for ray IO."""

import json
import os
import shutil
import tempfile
import time
import unittest

import pytest
import ray
from ray.dag.input_node import InputNode

from flow_io import ray_io


class RayTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=1, num_gpus=0)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.flow_file = os.path.join(self.temp_dir, 'flow_state.json')
        os.environ['FLOW_FILE'] = self.flow_file

    @pytest.fixture(autouse=True)
    def capsys(self, capsys):
        self.capsys = capsys

    def test_end_to_end_empty(self):
        with open(self.flow_file, 'w', encoding='UTF-8') as f:
            flow_contents = {
                'nodes': [
                    {
                        'nodeSpace': 'flow_io/ray_io'
                    },
                ],
                'outgoingEdges': {},
            }
            json.dump(flow_contents, f)
        node_dir = os.path.join(self.temp_dir, 'flow_io/ray_io')
        os.makedirs(os.path.join(self.temp_dir, 'flow_io/ray_io'))
        with open(os.path.join(node_dir, 'config.json'),
                  mode='w',
                  encoding='UTF-8') as f:
            json.dump({}, f)

        @ray.remote
        def ray_func(input):
            return input

        with InputNode() as dag_input:
            final_outputs = ray_io.output(dag_input)

        ray_io.input(*final_outputs, inputs=[1, 2, 3])

        time.sleep(10)

        out, _ = self.capsys.readouterr()

        self.assertIn('OUTPUT:  1', out)
        self.assertIn('OUTPUT:  2', out)
        self.assertIn('OUTPUT:  3', out)

    def tearDown(self):
        shutil.rmtree(self.temp_dir)


if __name__ == '__main__':
    unittest.main()
