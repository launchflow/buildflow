import unittest

import buildflow
from buildflow.api import NodePlan, ProcessorPlan
from buildflow.core.ray_io import pubsub_io


class TestPubSubIO(unittest.TestCase):

    def test_pubsub_io_plan(self):
        expected_plan = NodePlan(
            name="",
            processors=[
                ProcessorPlan(
                    name="ps_process",
                    source_resources={
                        "source_type": "PubSubSource",
                        "topic": "projects/p/topics/source",
                        "subscription": "projects/p/subscriptions/source",
                    },
                    sink_resources={
                        "sink_type": "PubSubSink",
                        "topic": "projects/p/topics/source",
                    },
                )
            ],
        )
        app = buildflow.ComputeNode()

        @app.processor(
            source=pubsub_io.PubSubSource(cloud="gcp",
                                          name="source",
                                          project_id="p"),
            sink=pubsub_io.PubSubSink(cloud="gcp",
                                      name="source",
                                      project_id="p"),
        )
        def ps_process(elem):
            pass

        plan = app.plan()
        self.assertEqual(expected_plan, plan)


if __name__ == "__main__":
    unittest.main()
