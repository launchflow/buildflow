import inspect
import os
import tempfile
import unittest
from typing import Dict

from buildflow.core.app.flow import Flow
from buildflow.core.processor.patterns.pipeline import PipelineProcessor
from buildflow.io.local.file import File
from buildflow.io.local.pulse import Pulse
from buildflow.types.local import FileFormat


class FlowTest(unittest.TestCase):
    def setUp(self) -> None:
        self.output_path = tempfile.mkstemp(suffix=".csv")[1]

    def tearDown(self) -> None:
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

            def _duplicate(self, payload: Dict[str, int]) -> Dict[str, int]:
                # Ensure we can still call inner methods
                new_payload = payload.copy()
                new_payload["field"] = new_payload["field"] + self.value_to_add
                return new_payload

            def process(self, payload: Dict[str, int]) -> Dict[str, int]:
                return self._duplicate(payload)

        self.assertIsInstance(MyPipeline, PipelineProcessor)

        full_arg_spec = inspect.getfullargspec(MyPipeline.process)
        self.assertEqual(full_arg_spec.args, ["self", "payload"])
        self.assertEqual(
            full_arg_spec.annotations,
            {"return": Dict[str, int], "payload": Dict[str, int]},
        )

        p = MyPipeline()
        p.setup()
        output = p.process({"field": 1})
        self.assertEqual(output, {"field": 2})


if __name__ == "__main__":
    unittest.main()
