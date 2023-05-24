"""Tests for depends.py"""

import unittest

import buildflow
from buildflow.runtime import depends


class DependsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.app = buildflow.Node()

    def test_processor_annotation_depends(self):
        @self.app.processor(
            source=buildflow.PubSubSource(
                cloud="gcp", name="p1_pubsub", project_id="unused"
            )
        )
        def processor1(element):
            pass

        pubsub_depends = depends.Depends(processor1)

        self.assertIsInstance(pubsub_depends, depends.PubSub)

    def test_process_class_depends(self):
        class MyProcessor(buildflow.Processor):
            @classmethod
            def source(cls):
                return buildflow.PubSubSource(
                    cloud="gcp", name="p1_pubsub", project_id="unused"
                )

            def process(self, payload: int):
                pass

        pubsub_depends = depends.Depends(MyProcessor)

        self.assertIsInstance(pubsub_depends, depends.PubSub)

    def test_unsupport_source_depends(self):
        @self.app.processor(source=buildflow.BigQuerySource())
        def processor1(element):
            pass

        with self.assertRaises(depends.UnsupportDepenendsSource):
            buildflow.Depends(processor1)

    def test_process_class_depends_no_class_method(self):
        class MyProcessor(buildflow.Processor):
            def source(cls):
                return buildflow.PubSubSource(
                    cloud="gcp", name="p1_pubsub", project_id="unused"
                )

            def process(self, payload: int):
                pass

        with self.assertRaises(depends.InvalidProcessorSource):
            depends.Depends(MyProcessor)


if __name__ == "__main__":
    unittest.main()
