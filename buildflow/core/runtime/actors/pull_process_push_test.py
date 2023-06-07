import asyncio
import os
from pathlib import Path
import tempfile
from typing import Dict, Iterable, List
import unittest

import pyarrow.csv as pcsv
import pytest

from buildflow.api import SinkType
from buildflow.api.io import SourceType
from buildflow.core.node import Node
from buildflow.core.processor.base import Processor
from buildflow.core.runtime.actors.pull_process_push import PullProcessPushActor
from buildflow.io.registry import Pulse, Files


def create_test_processor(output_path: str, pulsing_input: Iterable[Dict[str, int]]):
    class MyProcesesor(Processor):
        @classmethod
        def source(cls) -> SourceType:
            return Pulse(pulsing_input, pulse_interval_seconds=0.1)

        @classmethod
        def sink(self) -> SinkType:
            return Files(file_path=output_path, file_format="csv")

        def process(self, payload):
            return payload

    return MyProcesesor()


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
        actor = PullProcessPushActor.remote(
            processor=create_test_processor(
                self.output_path, [{"field": 1}, {"field": 2}]
            )
        )

        self.run_with_timeout(actor.run.remote())

        table = pcsv.read_csv(Path(self.output_path))
        table_list = table.to_pylist()
        self.assertGreaterEqual(len(table_list), 2)
        self.assertCountEqual([{"field": 1}, {"field": 2}], table_list[0:2])

    def test_end_to_end_with_processor_decorator(self):
        node = Node()

        @node.processor(
            source=Pulse([{"field": 1}, {"field": 2}], pulse_interval_seconds=0.1),
            sink=Files(file_path=self.output_path, file_format="csv"),
        )
        def process(payload):
            return payload

        actor = PullProcessPushActor.remote(processor=process)

        self.run_with_timeout(actor.run.remote())

        table = pcsv.read_csv(Path(self.output_path))
        table_list = table.to_pylist()
        self.assertGreaterEqual(len(table_list), 2)
        self.assertCountEqual([{"field": 1}, {"field": 2}], table_list[0:2])

    def test_end_to_end_with_processor_decorator_async(self):
        node = Node()

        @node.processor(
            source=Pulse([{"field": 1}, {"field": 2}], pulse_interval_seconds=0.1),
            sink=Files(file_path=self.output_path, file_format="csv"),
        )
        async def process(payload):
            return payload

        actor = PullProcessPushActor.remote(processor=process)

        self.run_with_timeout(actor.run.remote())

        table = pcsv.read_csv(Path(self.output_path))
        table_list = table.to_pylist()
        self.assertGreaterEqual(len(table_list), 2)
        self.assertCountEqual([{"field": 1}, {"field": 2}], table_list[0:2])

    def test_end_to_end_with_processor_decorator_flatten(self):
        node = Node()

        @node.processor(
            source=Pulse([{"field": 1}, {"field": 2}], pulse_interval_seconds=0.1),
            sink=Files(file_path=self.output_path, file_format="csv"),
        )
        async def process(payload) -> List[Dict[str, int]]:
            return [payload, payload]

        actor = PullProcessPushActor.remote(processor=process)

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
