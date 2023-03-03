"""Rays for ray IO."""

import unittest

import pytest

import buildflow


@pytest.mark.usefixtures('ray_fix')
class EmptyTest(unittest.TestCase):

    def setUp(self) -> None:
        self.flow = buildflow.Flow()

    def test_end_to_end_empty(self):

        @self.flow.processor(input_ref=buildflow.Empty(inputs=[1, 2, 3]))
        def process(elem):
            return elem

        output = self.flow.run()

        self.assertEqual(len(output), 1)
        self.assertEqual(list(output.values())[0], [1, 2, 3])

    def test_end_to_end_empty_multi_output(self):

        @self.flow.processor(input_ref=buildflow.Empty(inputs=[1, 2, 3]))
        def process(elem):
            return [elem, elem]

        output = self.flow.run()

        self.assertEqual(len(output), 1)
        self.assertEqual(list(output.values())[0], [[1, 1], [2, 2], [3, 3]])


if __name__ == '__main__':
    unittest.main()
