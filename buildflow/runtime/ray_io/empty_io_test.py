"""Rays for ray IO."""

import os
import shutil
import tempfile
import unittest

import ray

import buildflow as flow


class EmptyTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=1, num_gpus=0)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_end_to_end_empty(self):
        @flow.processor(input_ref=flow.Empty(inputs=[1, 2, 3]))
        def process(elem):
            return elem

        output = flow.run()

        self.assertEqual(len(output), 1)
        self.assertEqual(list(output.values())[0], [1, 2, 3])

    def test_end_to_end_empty_multi_output(self):
        @flow.processor(input_ref=flow.Empty(inputs=[1, 2, 3]))
        def process(elem):
            return [elem, elem]

        output = flow.run()

        self.assertEqual(len(output), 1)
        self.assertEqual(list(output.values())[0], [1, 1, 2, 2, 3, 3])


if __name__ == '__main__':
    unittest.main()
