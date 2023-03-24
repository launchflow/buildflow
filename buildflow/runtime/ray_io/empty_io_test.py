"""Rays for ray IO."""

import unittest

import pytest

import buildflow


@pytest.mark.usefixtures('ray_fix')
class EmptyTest(unittest.TestCase):

    def setUp(self) -> None:
        self.flow = buildflow.Flow()

    def test_end_to_end_empty(self):

        @self.flow.processor(source=buildflow.EmptySource(inputs=[1, 2, 3]))
        def process(elem):
            return elem

        output = self.flow.run().output()

        self.assertEqual(len(output), 1)
        self.assertEqual(output, {'process': {'local': [1, 2, 3]}})

    def test_end_to_end_empty_multi_output(self):

        @self.flow.processor(source=buildflow.EmptySource(inputs=[1, 2, 3]))
        def process(elem):
            return [elem, elem]

        output = self.flow.run().output()

        self.assertEqual(len(output), 1)
        self.assertEqual(output,
                         {'process': {
                             'local': [1, 1, 2, 2, 3, 3]
                         }})

    def test_end_to_end_with_class(self):

        class MyProcessor(buildflow.Processor):

            def source(self):
                return buildflow.EmptySource([1, 2, 3])

            def process(self, num: int):
                return num

        output = self.flow.run(MyProcessor()).output()

        self.assertEqual(len(output), 1)
        self.assertEqual(output, {'MyProcessor': {'local': [1, 2, 3]}})

    def test_end_to_end_empty_multiple_processors(self):

        @self.flow.processor(source=buildflow.EmptySource(inputs=[1, 2, 3]))
        def process1(elem):
            return elem

        @self.flow.processor(source=buildflow.EmptySource(inputs=[4, 5, 6]))
        def process2(elem):
            return elem

        output = self.flow.run().output()

        self.assertEqual(len(output), 2)
        self.assertEqual(output, {
            'process1': {
                'local': [1, 2, 3]
            },
            'process2': {
                'local': [4, 5, 6]
            }
        })


if __name__ == '__main__':
    unittest.main()
