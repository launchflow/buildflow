import inspect
import os
import shutil
import tempfile
import unittest
from typing import Dict

import pulumi

from buildflow.core.app.flow import Flow
from buildflow.core.processor.patterns.pipeline import PipelineProcessor
from buildflow.io.aws.sqs import SQSQueue
from buildflow.io.gcp.bigquery_dataset import BigQueryDataset
from buildflow.io.gcp.bigquery_table import BigQueryTable
from buildflow.io.gcp.pubsub_subscription import GCPPubSubSubscription
from buildflow.io.gcp.pubsub_topic import GCPPubSubTopic
from buildflow.io.local.file import File
from buildflow.io.local.pulse import Pulse
from buildflow.types.portable import FileFormat


class MyMocks(pulumi.runtime.Mocks):
    def new_resource(self, args: pulumi.runtime.MockResourceArgs):
        return [args.name + "_id", args.inputs]

    def call(self, args: pulumi.runtime.MockCallArgs):
        return {}


class FlowTest(unittest.TestCase):
    def setUp(self) -> None:
        self.output_path = tempfile.mkstemp(suffix=".csv")[1]
        self.tempdir = tempfile.mkdtemp()

    def tearDown(self) -> None:
        shutil.rmtree(self.tempdir)
        os.remove(self.output_path)

    def test_flow_process_fn_decorator(self):
        app = Flow()

        @app.pipeline(
            source=Pulse([{"field": 1}, {"field": 2}], pulse_interval_seconds=0.1),
            sink=File(file_path=self.output_path, file_format=FileFormat.CSV),
        )
        def process(payload: Dict[str, int]) -> Dict[str, int]:
            return payload

        self.assertIsInstance(process, PipelineProcessor)

        full_arg_spec = inspect.getfullargspec(process.process)
        self.assertEqual(full_arg_spec.args, ["self", "payload"])
        self.assertEqual(
            full_arg_spec.annotations,
            {"return": Dict[str, int], "payload": Dict[str, int]},
        )

    def test_flow_process_class_decorator(self):
        app = Flow()

        @app.pipeline(
            source=Pulse([{"field": 1}, {"field": 2}], pulse_interval_seconds=0.1),
            sink=File(file_path=self.output_path, file_format=FileFormat.CSV),
        )
        class MyPipeline:
            def setup(self):
                self.value_to_add = 1
                self.teardown_called = False

            def _duplicate(self, payload: Dict[str, int]) -> Dict[str, int]:
                # Ensure we can still call inner methods
                new_payload = payload.copy()
                new_payload["field"] = new_payload["field"] + self.value_to_add
                return new_payload

            def process(self, payload: Dict[str, int]) -> Dict[str, int]:
                return self._duplicate(payload)

            def teardown(self):
                self.teardown_called = True

        self.assertIsInstance(MyPipeline, PipelineProcessor)

        full_arg_spec = inspect.getfullargspec(MyPipeline.process)
        self.assertEqual(full_arg_spec.args, ["self", "payload"])
        self.assertEqual(
            full_arg_spec.annotations,
            {"return": Dict[str, int], "payload": Dict[str, int]},
        )

        p = MyPipeline()
        p.setup()
        p.teardown()

        output = p.process({"field": 1})
        self.assertEqual(output, {"field": 2})
        self.assertEqual(p.teardown_called, True)

    @pulumi.runtime.test
    def test_pulumi_program(self):
        app = Flow()

        bigquery_dataset = BigQueryDataset(
            project_id="project_id", dataset_name="dataset_name"
        ).options(managed=True)
        bigquery_table = BigQueryTable(
            bigquery_dataset, table_name="table_name"
        ).options(managed=True)
        pubsub_topic = GCPPubSubTopic(
            project_id="project_id", topic_name="topic_name"
        ).options(managed=False)
        pubsub_subscription = GCPPubSubSubscription(
            project_id="project_id", subscription_name="subscription_name"
        ).options(managed=True, topic=pubsub_topic)

        @app.pipeline(source=pubsub_subscription, sink=bigquery_table)
        def process(payload: Dict[str, int]) -> Dict[str, int]:
            return payload

        stack = "test-stack"
        project = "test-project"
        pulumi.runtime.set_mocks(MyMocks(), project=project, stack=stack, preview=False)

        process.pulumi_program()

        primitive_cache = app._primitive_cache

        # Ensure all the primitives get visited.
        # We don't actually validate what pulumi resources are created here, since
        # those should have their own tests.
        self.assertEqual(len(primitive_cache.cache), 3)
        self.assertIn(bigquery_dataset, primitive_cache)
        self.assertIn(bigquery_table, primitive_cache)
        self.assertIn(pubsub_subscription, primitive_cache)
        self.assertNotIn(pubsub_topic, primitive_cache)

    @pulumi.runtime.test
    def test_pulumi_program_same_sink(self):
        app = Flow()

        queue = SQSQueue(queue_name="queue_name").options(managed=True)

        @app.pipeline(source=queue, sink=queue)
        def process(payload: Dict[str, int]) -> Dict[str, int]:
            return payload

        stack = "test-stack"
        project = "test-project"
        pulumi.runtime.set_mocks(MyMocks(), project=project, stack=stack, preview=False)

        process.pulumi_program()

        primitive_cache = app._primitive_cache

        # Ensure all the primitives get visited.
        # We don't actually validate what pulumi resources are created here, since
        # those should have their own tests.
        self.assertEqual(len(primitive_cache.cache), 1)


if __name__ == "__main__":
    unittest.main()
