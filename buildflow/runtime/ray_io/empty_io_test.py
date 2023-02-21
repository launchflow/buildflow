"""Rays for ray IO."""

import os
import shutil
import tempfile
import unittest

import ray

from buildflow import flow_state
from buildflow import resources
from buildflow import io


class EmptyTest(unittest.TestCase):

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
        flow_state.init(
            config={
                'input': resources.Empty(inputs=[1, 2, 3]),
                'outputs': [resources.Empty()]
            })

        @ray.remote
        def process(elem):
            return elem

        output = io.run(process.remote)

        # TODO: why are these nested lists?
        self.assertEqual(output, [1, 2, 3])

    def tearDown(self):
        shutil.rmtree(self.temp_dir)


if __name__ == '__main__':
    unittest.main()
