import dataclasses
import inspect
import os
import shutil
import tempfile
import unittest
from typing import Dict

import pulumi

from buildflow.core.app.flow import Flow
from buildflow.core.options.flow_options import FlowOptions
from buildflow.core.processor.patterns.consumer import ConsumerProcessor
from buildflow.io.gcp.bigquery_dataset import BigQueryDataset
from buildflow.io.gcp.bigquery_table import BigQueryTable
from buildflow.io.gcp.pubsub_subscription import GCPPubSubSubscription
from buildflow.io.gcp.pubsub_topic import GCPPubSubTopic
from buildflow.io.local.file import File
from buildflow.io.local.pulse import Pulse
from buildflow.types.portable import FileFormat


@dataclasses.dataclass
class MySchema:
    field: int


class MyMocks(pulumi.runtime.Mocks):
    def new_resource(self, args: pulumi.runtime.MockResourceArgs):
        return [args.name + "_id", args.inputs]

    def call(self, args: pulumi.runtime.MockCallArgs):
        return {}


class FlowTest(unittest.TestCase):
    def setUp(self) -> None:
        self.output_path = tempfile.mkstemp(suffix=".csv")[1]
        self.tempdir = tempfile.mkdtemp()
        # write buildflow config
        buildflow_yaml = """\
app: end-to-end-tests
pulumi_config:
  default_stack: local
  pulumi_home: .buildflow/_pulumi
  stacks:
    - name: local
      backend_url: file://.buildflow/_pulumi/local
entry_point: main:app
"""
        self.buildflow_yaml_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "buildflow.yaml"
        )
        with open(self.buildflow_yaml_path, "w") as f:
            f.write(buildflow_yaml)

    def tearDown(self) -> None:
        shutil.rmtree(self.tempdir)
        os.remove(self.output_path)
        os.remove(self.buildflow_yaml_path)

    def test_flow_process_fn_decorator(self):
        app = Flow()

        @app.consumer(
            source=Pulse([{"field": 1}, {"field": 2}], pulse_interval_seconds=0.1),
            sink=File(file_path=self.output_path, file_format=FileFormat.CSV),
        )
        def process(payload: Dict[str, int]) -> Dict[str, int]:
            return payload

        self.assertIsInstance(process, ConsumerProcessor)

        full_arg_spec = inspect.getfullargspec(process.process)
        self.assertEqual(full_arg_spec.args, ["self", "payload"])
        self.assertEqual(
            full_arg_spec.annotations,
            {"return": Dict[str, int], "payload": Dict[str, int]},
        )

    def test_flow_process_class_decorator(self):
        app = Flow()

        @app.consumer(
            source=Pulse([{"field": 1}, {"field": 2}], pulse_interval_seconds=0.1),
            sink=File(file_path=self.output_path, file_format=FileFormat.CSV),
        )
        class MyConsumer:
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

        self.assertIsInstance(MyConsumer, ConsumerProcessor)

        full_arg_spec = inspect.getfullargspec(MyConsumer.process)
        self.assertEqual(full_arg_spec.args, ["self", "payload"])
        self.assertEqual(
            full_arg_spec.annotations,
            {"return": Dict[str, int], "payload": Dict[str, int]},
        )

        p = MyConsumer()
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
        )
        bigquery_table1 = BigQueryTable(
            bigquery_dataset, table_name="table_name1"
        ).options(schema=MySchema)
        bigquery_table2 = BigQueryTable(
            bigquery_dataset, table_name="table_name2"
        ).options(schema=MySchema)
        pubsub_topic = GCPPubSubTopic(project_id="project_id", topic_name="topic_name")
        pubsub_subscription = GCPPubSubSubscription(
            project_id="project_id", subscription_name="subscription_name"
        ).options(topic=pubsub_topic)

        app.manage(
            bigquery_dataset, bigquery_table1, bigquery_table2, pubsub_subscription
        )

        @app.consumer(source=pubsub_subscription, sink=bigquery_table1)
        def process(payload: Dict[str, int]) -> Dict[str, int]:
            return payload

        stack = "test-stack"
        project = "test-project"
        pulumi.runtime.set_mocks(MyMocks(), project=project, stack=stack, preview=False)

        pulumi_resources = app._pulumi_program()

        # Ensure all the primitives get visited.
        # We don't actually validate what pulumi resources are created here, since
        # those should have their own tests.
        self.assertEqual(len(pulumi_resources), 4)

    def test_flowstate_consumer(self):
        app = Flow()

        bigquery_dataset = BigQueryDataset(
            project_id="project_id", dataset_name="dataset_name"
        )
        bigquery_table = BigQueryTable(
            bigquery_dataset, table_name="table_name"
        ).options(schema=MySchema)
        pubsub_topic = GCPPubSubTopic(project_id="project_id", topic_name="topic_name")
        pubsub_subscription = GCPPubSubSubscription(
            project_id="project_id", subscription_name="subscription_name"
        ).options(topic=pubsub_topic)

        PST = pubsub_topic.dependency()

        app.manage(bigquery_dataset, bigquery_table, pubsub_subscription)

        @app.consumer(source=pubsub_subscription, sink=bigquery_table)
        def process(payload: Dict[str, int], pst: PST) -> Dict[str, int]:
            return payload

        flowstate = app.flowstate()

        self.assertEqual(flowstate.flow_id, app.flow_id)
        self.assertEqual(len(flowstate.processor_group_states), 1)
        self.assertEqual(len(flowstate.primitive_states), 4)
        self.assertEqual("local", flowstate.stack)

    def test_flowstate_collector(self):
        app = Flow(flow_options=FlowOptions(stack="a-diff-stack"))

        bigquery_dataset = BigQueryDataset(
            project_id="project_id", dataset_name="dataset_name"
        )
        bigquery_table = BigQueryTable(
            bigquery_dataset, table_name="table_name"
        ).options(schema=MySchema)
        pubsub_topic = GCPPubSubTopic(project_id="project_id", topic_name="topic_name")

        PST = pubsub_topic.dependency()

        app.manage(bigquery_table)

        @app.collector(route="/", method="POST", sink=bigquery_table)
        def process(payload: Dict[str, int], pst: PST) -> Dict[str, int]:
            return payload

        flowstate = app.flowstate()

        self.assertEqual(flowstate.flow_id, app.flow_id)
        self.assertEqual(len(flowstate.processor_group_states), 1)
        self.assertEqual(len(flowstate.primitive_states), 3)
        self.assertEqual("a-diff-stack", flowstate.stack)

    def test_flowstate_endpoint(self):
        app = Flow()
        service = app.service()

        pubsub_topic = GCPPubSubTopic(project_id="project_id", topic_name="topic_name")

        PST = pubsub_topic.dependency()

        @service.endpoint(route="/", method="POST")
        def process(payload: Dict[str, int], pst: PST) -> Dict[str, int]:
            return payload

        flowstate = app.flowstate()

        self.assertEqual(flowstate.flow_id, app.flow_id)
        self.assertEqual(len(flowstate.processor_group_states), 1)
        self.assertEqual(len(flowstate.primitive_states), 1)


if __name__ == "__main__":
    unittest.main()
