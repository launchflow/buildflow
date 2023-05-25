"""Rays for ray IO."""

from dataclasses import asdict, dataclass
import unittest

import pytest

import buildflow


@dataclass
class Input:
    a: int


@pytest.mark.usefixtures("ray_fix")
class EmptyTest(unittest.TestCase):
    def setUp(self) -> None:
        self.app = buildflow.Node()

    def test_end_to_end_empty(self):
        @self.app.processor(source=buildflow.EmptySource(inputs=[1, 2, 3]))
        def process(elem):
            return elem

        output = self.app.run()

        self.assertEqual(len(output), 1)
        self.assertEqual(output, {"process": {"local": [1, 2, 3]}})

    def test_end_to_end_empty_multi_output(self):
        @self.app.processor(source=buildflow.EmptySource(inputs=[1, 2, 3]))
        def process(elem):
            return [elem, elem]

        output = self.app.run()

        self.assertEqual(len(output), 1)
        self.assertEqual(output, {"process": {"local": [1, 1, 2, 2, 3, 3]}})

    def test_end_to_end_with_class(self):
        class MyProcessor(buildflow.Processor):
            def source(self):
                return buildflow.EmptySource([1, 2, 3])

            def process(self, num: int):
                return num

        self.app.add_processor(MyProcessor())

        output = self.app.run()

        self.assertEqual(len(output), 1)
        self.assertEqual(output, {"MyProcessor": {"local": [1, 2, 3]}})

    def test_end_to_end_empty_multiple_processors(self):
        @self.app.processor(source=buildflow.EmptySource(inputs=[1, 2, 3]))
        def process1(elem):
            return elem

        @self.app.processor(source=buildflow.EmptySource(inputs=[4, 5, 6]))
        def process2(elem):
            return elem

        output = self.app.run()

        self.assertEqual(len(output), 2)
        self.assertEqual(
            output, {"process1": {"local": [1, 2, 3]}, "process2": {"local": [4, 5, 6]}}
        )

    def test_end_to_end_with_data_class(self):
        @self.app.processor(
            source=buildflow.EmptySource(
                inputs=[asdict(Input(1)), asdict(Input(2)), asdict(Input(3))]
            )
        )
        def process(elem: Input) -> Input:
            return asdict(elem)

        output = self.app.run()

        self.assertEqual(len(output), 1)
        self.assertEqual(output, {"process": {"local": [{"a": 1}, {"a": 2}, {"a": 3}]}})

    def test_end_to_end_with_leave_existing_data_class(self):
        @self.app.processor(
            source=buildflow.EmptySource(
                inputs=[Input(1), asdict(Input(2)), asdict(Input(3))]
            )
        )
        def process(elem: Input) -> Input:
            return asdict(elem)

        output = self.app.run()

        self.assertEqual(len(output), 1)
        self.assertEqual(output, {"process": {"local": [{"a": 1}, {"a": 2}, {"a": 3}]}})


if __name__ == "__main__":
    unittest.main()
