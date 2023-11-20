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
from buildflow.core.app.runtime._runtime import RuntimeStatus
from buildflow.core.app.runtime.actors.consumer_pattern.pull_process_push import (
    PullProcessPushActor,
)
from buildflow.core.processor.patterns.consumer import ConsumerGroup
from buildflow.io.local.file import File
from buildflow.io.local.pulse import Pulse
from buildflow.types.portable import FileFormat


@pytest.mark.usefixtures("ray")
class PullProcessPushTest(unittest.IsolatedAsyncioTestCase):
    def get_output_file(self) -> str:
        files = os.listdir(self.output_dir)
        self.assertEqual(1, len(files))
        return os.path.join(self.output_dir, files[0])

    def setUp(self) -> None:
        self.output_dir = tempfile.mkdtemp()
        self.output_path = os.path.join(self.output_dir, "test.csv")

    def tearDown(self) -> None:
        shutil.rmtree(self.output_dir)

    async def run_with_timeout(self, coro, timeout: int = 5, fail: bool = False):
        """Run a coroutine synchronously."""
        try:
            return await asyncio.wait_for(coro, timeout=timeout)
        except asyncio.TimeoutError:
            if fail:
                raise
            return

    async def run_for_time(self, coro, time: int = 5):
        completed, pending = await asyncio.wait(
            [coro], timeout=time, return_when="FIRST_EXCEPTION"
        )
        if completed:
            # This general should only happen when there was an exception so
            # we want to raise it to make the test failure more obvious.
            completed.pop().result()
        if pending:
            return pending.pop()

    async def test_end_to_end_with_processor_class(self):
        app = Flow()

        @app.consumer(
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
            run_id="test-run",
            processor_group=ConsumerGroup(group_id="g", processors=[MyProcesesor]),
            replica_id="1",
            flow_dependencies={},
        )
        await actor.initialize.remote()

        await self.run_with_timeout(actor.run.remote())

        final_file = self.get_output_file()

        table = pcsv.read_csv(Path(final_file))
        table_list = table.to_pylist()
        self.assertGreaterEqual(len(table_list), 2)
        self.assertCountEqual([{"field": 2}, {"field": 3}], table_list[0:2])

        await self.run_with_timeout(actor.drain.remote())

    async def test_end_to_end_with_processor_decorator(self):
        app = Flow()

        @app.consumer(
            source=Pulse([{"field": 1}, {"field": 2}], pulse_interval_seconds=0.1),
            sink=File(file_path=self.output_path, file_format=FileFormat.CSV),
        )
        def process(payload):
            return payload

        actor = PullProcessPushActor.remote(
            run_id="test-run",
            processor_group=ConsumerGroup(group_id="g", processors=[process]),
            replica_id="1",
            flow_dependencies={},
        )
        await actor.initialize.remote()

        await self.run_with_timeout(actor.run.remote())

        final_file = self.get_output_file()
        table = pcsv.read_csv(Path(final_file))
        table_list = table.to_pylist()
        self.assertGreaterEqual(len(table_list), 2)
        self.assertCountEqual([{"field": 1}, {"field": 2}], table_list[0:2])

        await self.run_with_timeout(actor.drain.remote())

    async def test_end_to_end_with_processor_decorator_async(self):
        app = Flow()

        @app.consumer(
            source=Pulse([{"field": 1}, {"field": 2}], pulse_interval_seconds=0.1),
            sink=File(file_path=self.output_path, file_format=FileFormat.CSV),
        )
        async def process(payload):
            return payload

        actor = PullProcessPushActor.remote(
            run_id="test-run",
            processor_group=ConsumerGroup(group_id="g", processors=[process]),
            replica_id="1",
            flow_dependencies={},
        )
        await actor.initialize.remote()

        await self.run_with_timeout(actor.run.remote())

        final_file = self.get_output_file()
        table = pcsv.read_csv(Path(final_file))
        table_list = table.to_pylist()
        self.assertGreaterEqual(len(table_list), 2)
        self.assertCountEqual([{"field": 1}, {"field": 2}], table_list[0:2])

        await self.run_with_timeout(actor.drain.remote())

    async def test_end_to_end_with_processor_decorator_flatten(self):
        app = Flow()

        @app.consumer(
            source=Pulse([{"field": 1}, {"field": 2}], pulse_interval_seconds=0.1),
            sink=File(file_path=self.output_path, file_format=FileFormat.CSV),
        )
        async def process(payload) -> List[Dict[str, int]]:
            return [payload, payload]

        actor = PullProcessPushActor.remote(
            run_id="test-run",
            processor_group=ConsumerGroup(group_id="g", processors=[process]),
            replica_id="1",
            flow_dependencies={},
        )
        await actor.initialize.remote()

        await self.run_with_timeout(actor.run.remote())

        final_file = self.get_output_file()
        table = pcsv.read_csv(Path(final_file))
        table_list = table.to_pylist()
        self.assertGreaterEqual(len(table_list), 4)

        first_two = table_list[0:2]
        # Assert that the first two rows are the same.
        # This should always be true because we are returning the same payload twice.
        self.assertTrue(first_two[0] == first_two[1])

        await self.run_with_timeout(actor.drain.remote())

    async def test_end_to_end_with_processor_drain_multi_thread(self):
        app = Flow()

        @app.consumer(
            source=Pulse([{"field": 1}, {"field": 2}], pulse_interval_seconds=0.1),
            sink=File(file_path=self.output_path, file_format=FileFormat.CSV),
        )
        class MyProcesesor:
            def setup(self):
                self.calls = 0

            async def process(self, payload: Dict[str, int]) -> Dict[str, int]:
                # This is set to some astronimical number to ensure that the
                # this thread is still processing when the drain is called.
                if self.calls == 0:
                    self.calls += 1
                    await asyncio.sleep(10000)
                return payload

        actor = PullProcessPushActor.remote(
            run_id="test-run",
            processor_group=ConsumerGroup(group_id="g", processors=[MyProcesesor]),
            replica_id="1",
            flow_dependencies={},
        )
        await actor.initialize.remote()

        coro1 = await self.run_for_time(actor.run.remote())
        coro2 = await self.run_for_time(actor.run.remote())
        num_active_threads = await actor.num_active_threads.remote()
        self.assertEqual(2, num_active_threads)
        await self.run_for_time(actor.drain.remote())
        await self.run_for_time(coro1)
        await self.run_for_time(coro2)
        num_active_threads = await actor.num_active_threads.remote()
        self.assertEqual(1, num_active_threads)
        status = await actor.status.remote()
        self.assertEqual(RuntimeStatus.DRAINING, status)
        await self.run_with_timeout(actor.drain.remote())

    async def test_end_to_end_with_processor_fully_drained(self):
        app = Flow()

        @app.consumer(
            source=Pulse([{"field": 1}, {"field": 2}], pulse_interval_seconds=0.1),
            sink=File(file_path=self.output_path, file_format=FileFormat.CSV),
        )
        class MyProcesesor:
            def setup(self):
                self.calls = 0

            async def process(self, payload: Dict[str, int]) -> Dict[str, int]:
                return payload

        actor = PullProcessPushActor.remote(
            run_id="test-run",
            processor_group=ConsumerGroup(group_id="g", processors=[MyProcesesor]),
            replica_id="1",
            flow_dependencies={},
        )
        await actor.initialize.remote()

        coro1 = await self.run_for_time(actor.run.remote())
        coro2 = await self.run_for_time(actor.run.remote())
        num_active_threads = await actor.num_active_threads.remote()
        self.assertEqual(2, num_active_threads)
        await self.run_for_time(actor.drain.remote())
        await self.run_for_time(coro1)
        await self.run_for_time(coro2)
        num_active_threads = await actor.num_active_threads.remote()
        self.assertEqual(0, num_active_threads)
        status = await actor.status.remote()
        self.assertEqual(RuntimeStatus.DRAINED, status)
        await self.run_with_timeout(actor.drain.remote())


if __name__ == "__main__":
    unittest.main()
