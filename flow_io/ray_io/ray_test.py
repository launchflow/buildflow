"""Rays for ray IO."""

import json
import os
import shutil
import tempfile
import unittest

import ray

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
        self.deployment_file = os.path.join(self.temp_dir, 'deployment.json')
        os.environ['FLOW_FILE'] = self.flow_file
        os.environ['FLOW_DEPLOYMENT_FILE'] = self.deployment_file

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
        with open(self.deployment_file, 'w', encoding='UTF-8') as f:
            json.dump({'nodeDeployments': {}}, f)

        sink = ray_io.sink()
        source = ray_io.source(sink.write, inputs=[1, 2, 3])
        output = ray.get(source.run.remote())

        # TODO: why are these nested lists?
        self.assertEqual(output, [[1], [2], [3]])

    def tearDown(self):
        shutil.rmtree(self.temp_dir)


if __name__ == '__main__':
    unittest.main()
