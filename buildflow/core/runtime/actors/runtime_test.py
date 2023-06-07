import asyncio
import os
from pathlib import Path
import tempfile
import time
import unittest

import pyarrow.csv as pcsv
import pytest

from buildflow.core.node import Node
from buildflow.core.runtime.config import RuntimeConfig
from buildflow.core.runtime.actors.runtime import RuntimeActor
from buildflow.io.registry import Pulse, Files


@pytest.mark.usefixtures("ray_fix")
@pytest.mark.usefixtures("event_loop_instance")
class RunTimeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.output_path = tempfile.mkstemp(suffix=".csv")[1]

    def tearDown(self) -> None:
        os.remove(self.output_path)

    def run_with_timeout(self, coro):
        """Run a coroutine synchronously."""
        self.event_loop.run_until_complete(asyncio.wait_for(coro, timeout=5))

    def test_runtime_end_to_end(self):
        node = Node()

        @node.processor(
            source=Pulse([{"field": 1}, {"field": 2}], pulse_interval_seconds=0.1),
            sink=Files(file_path=self.output_path, file_format="csv"),
        )
        def process(payload):
            return payload

        actor = RuntimeActor.remote(config=RuntimeConfig.DEBUG())

        actor.run.remote(processors=[process])

        time.sleep(15)

        self.run_with_timeout(actor.drain.remote())

        table = pcsv.read_csv(Path(self.output_path))
        table_list = table.to_pylist()
        self.assertGreaterEqual(len(table_list), 2)
        self.assertCountEqual([{"field": 1}, {"field": 2}], table_list[0:2])


if __name__ == "__main__":
    unittest.main()
