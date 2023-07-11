import asyncio
import os
import tempfile
import time
import unittest
from pathlib import Path

import pyarrow.csv as pcsv
import pytest

from buildflow.core.app.flow import Flow
from buildflow.core.app.runtime.actors.runtime import RuntimeActor
from buildflow.core.io.local.file import File
from buildflow.core.io.local.pulse import Pulse
from buildflow.core.options import ProcessorOptions, RuntimeOptions
from buildflow.core.types.local_types import FileFormat


@pytest.mark.usefixtures("ray_fix")
@pytest.mark.usefixtures("event_loop_instance")
class RunTimeTest(unittest.TestCase):
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

    def test_runtime_end_to_end(self):
        app = Flow()

        @app.pipeline(
            source=Pulse([{"field": 1}, {"field": 2}], pulse_interval_seconds=0.1),
            sink=File(file_path=self.output_path, file_format=FileFormat.CSV),
        )
        def process(payload):
            return payload

        runtime_options = RuntimeOptions.default()
        runtime_options.processor_options["process"] = ProcessorOptions.default()
        # NOTE: We need to set the num_cpus to a small value since pytest limits the
        # number of CPUs available to the test process. (I didnt actually verify this
        # but I think its true)
        runtime_options.processor_options["process"].num_cpus = 0.1
        actor = RuntimeActor.remote(run_id="test-run", runtime_options=runtime_options)

        actor.run.remote(processors=[process])

        time.sleep(5)

        self.run_with_timeout(actor.drain.remote())

        table = pcsv.read_csv(Path(self.output_path))
        table_list = table.to_pylist()
        self.assertGreaterEqual(len(table_list), 2)
        self.assertCountEqual([{"field": 1}, {"field": 2}], table_list[0:2])


if __name__ == "__main__":
    unittest.main()
