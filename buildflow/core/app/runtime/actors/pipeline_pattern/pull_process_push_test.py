import asyncio
import os
import shutil
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
from buildflow.io.local.file import File
from buildflow.io.local.pulse import Pulse
from buildflow.types.portable import FileFormat


@pytest.mark.usefixtures("ray_fix")
@pytest.mark.usefixtures("event_loop_instance")
class PullProcessPushTest(unittest.TestCase):
    def get_output_file(self) -> str:
        files = os.listdir(self.output_dir)
        self.assertEqual(1, len(files))
        return os.path.join(self.output_dir, files[0])

    def setUp(self) -> None:
        self.output_dir = tempfile.mkdtemp()
        self.output_path = os.path.join(self.output_dir, "test.csv")

    def tearDown(self) -> None:
        shutil.rmtree(self.output_dir)

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

        final_file = self.get_output_file()

        table = pcsv.read_csv(Path(final_file))
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

        final_file = self.get_output_file()
        table = pcsv.read_csv(Path(final_file))
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

        final_file = self.get_output_file()
        table = pcsv.read_csv(Path(final_file))
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

        final_file = self.get_output_file()
        table = pcsv.read_csv(Path(final_file))
        table_list = table.to_pylist()
        self.assertGreaterEqual(len(table_list), 4)

        first_two = table_list[0:2]
        # Assert that the first two rows are the same.
        # This should always be true because we are returning the same payload twice.
        self.assertTrue(first_two[0] == first_two[1])


if __name__ == "__main__":
    unittest.main()
