import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from typing import Dict, List

import pyarrow.csv as pcsv
import pytest

from buildflow.core.app.flow import Flow
from buildflow.core.app.runtime.actors.pipeline_pattern.pull_process_push import (
    PullProcessPushActor,
)
from buildflow.core.io.local.file import File
from buildflow.core.io.local.pulse import Pulse
from buildflow.core.types.local_types import FileFormat


@pytest.mark.usefixtures("ray_fix")
@pytest.mark.usefixtures("event_loop_instance")
class PullProcessPushTest(unittest.TestCase):
    def setUp(self) -> None:
        self.output_path = tempfile.mkstemp(suffix=".csv")[1]

    def tearDown(self) -> None:
        os.remove(self.output_path)

    def run_with_timeout(self, coro):
        """Run a coroutine synchronously."""
        try:
            self.event_loop.run_until_complete(asyncio.wait_for(coro, timeout=5))
        except asyncio.TimeoutError:
            return

    def test_end_to_end_with_processor_class(self):
        app = Flow()

        @app.pipeline(
            source=Pulse([{"field": 1}, {"field": 2}], pulse_interval_seconds=0.1),
            sink=File(file_path=self.output_path, file_format=FileFormat.CSV),
        )
        class MyProcesesor:
            def setup(self):
                self.value_to_add = 1

            def process(self, payload: Dict[str, int]) -> Dict[str, int]:
                new_payload = payload.copy()
                new_payload["field"] = new_payload["field"] + self.value_to_add
                return new_payload

        actor = PullProcessPushActor.remote(
            run_id="test-run", processor=MyProcesesor, replica_id="1"
        )

        self.run_with_timeout(actor.run.remote())

        table = pcsv.read_csv(Path(self.output_path))
        table_list = table.to_pylist()
        self.assertGreaterEqual(len(table_list), 2)
        self.assertCountEqual([{"field": 2}, {"field": 3}], table_list[0:2])

    def test_end_to_end_with_processor_decorator(self):
        app = Flow()

        @app.pipeline(
            source=Pulse([{"field": 1}, {"field": 2}], pulse_interval_seconds=0.1),
            sink=File(file_path=self.output_path, file_format=FileFormat.CSV),
        )
        def process(payload):
            return payload

        actor = PullProcessPushActor.remote(
            run_id="test-run",
            processor=process,
            replica_id="1",
        )

        self.run_with_timeout(actor.run.remote())

        table = pcsv.read_csv(Path(self.output_path))
        table_list = table.to_pylist()
        self.assertGreaterEqual(len(table_list), 2)
        self.assertCountEqual([{"field": 1}, {"field": 2}], table_list[0:2])

    def test_end_to_end_with_processor_decorator_async(self):
        app = Flow()

        @app.pipeline(
            source=Pulse([{"field": 1}, {"field": 2}], pulse_interval_seconds=0.1),
            sink=File(file_path=self.output_path, file_format=FileFormat.CSV),
        )
        async def process(payload):
            return payload

        actor = PullProcessPushActor.remote(
            run_id="test-run",
            processor=process,
            replica_id="1",
        )

        self.run_with_timeout(actor.run.remote())

        table = pcsv.read_csv(Path(self.output_path))
        table_list = table.to_pylist()
        self.assertGreaterEqual(len(table_list), 2)
        self.assertCountEqual([{"field": 1}, {"field": 2}], table_list[0:2])

    def test_end_to_end_with_processor_decorator_flatten(self):
        app = Flow()

        @app.pipeline(
            source=Pulse([{"field": 1}, {"field": 2}], pulse_interval_seconds=0.1),
            sink=File(file_path=self.output_path, file_format=FileFormat.CSV),
        )
        async def process(payload) -> List[Dict[str, int]]:
            return [payload, payload]

        actor = PullProcessPushActor.remote(
            run_id="test-run",
            processor=process,
            replica_id="1",
        )

        self.run_with_timeout(actor.run.remote())

        table = pcsv.read_csv(Path(self.output_path))
        table_list = table.to_pylist()
        self.assertGreaterEqual(len(table_list), 4)

        first_two = table_list[0:2]
        # Assert that the first two rows are the same.
        # This should always be true because we are returning the same payload twice.
        self.assertTrue(first_two[0] == first_two[1])


if __name__ == "__main__":
    unittest.main()
