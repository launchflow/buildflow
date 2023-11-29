import asyncio
import os
import shutil
import signal
import tempfile
import unittest
from pathlib import Path

import pyarrow.csv as pcsv
import pytest
from ray.util.state import list_actors

from buildflow.core.app.flow import Flow
from buildflow.core.app.runtime.actors.runtime import RuntimeActor
from buildflow.core.options import ProcessorOptions, RuntimeOptions
from buildflow.core.processor.patterns.consumer import ConsumerGroup
from buildflow.io.local.file import File
from buildflow.io.local.pulse import Pulse
from buildflow.io.local.testing.pulse_with_backlog import PulseWithBacklog
from buildflow.types.portable import FileFormat


@pytest.mark.usefixtures("ray")
class RunTimeTest(unittest.IsolatedAsyncioTestCase):
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

    async def run_for_time(self, coro, run_time: int = 5):
        completed, pending = await asyncio.wait(
            [coro], timeout=run_time, return_when="FIRST_EXCEPTION"
        )
        if completed:
            # This general should only happen when there was an exception so
            # we want to raise it to make the test failure more obvious.
            completed.pop().result()
        if pending:
            return pending.pop()

    def assertInStderr(self, expected_match: str):
        _, err = self._capfd.readouterr()
        split_err = err.split("\n")
        found_match = False
        for line in split_err:
            if expected_match in line:
                found_match = True
                break
        assert found_match, f"Expected to find `{expected_match}` in stderr."

    @pytest.fixture(autouse=True)
    def inject_fixtures(self, capfd: pytest.CaptureFixture[str]):
        self._capfd = capfd

    async def test_runtime_end_to_end(self):
        app = Flow()

        @app.consumer(
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
        runtime_options.processor_options["process"].num_cpus = 0.5
        actor = RuntimeActor.remote(
            run_id="test-run",
            runtime_options=runtime_options,
            flow_dependencies={},
        )

        await actor.run.remote(
            processor_groups=[ConsumerGroup(processors=[process], group_id="process")],
            serve_port=0,
            serve_host="unused",
            event_subscriber=None,
        )
        await asyncio.sleep(15)

        await self.run_with_timeout(actor.drain.remote(), fail=True)

        files = os.listdir(self.output_dir)
        self.assertEqual(len(files), 1)
        csv_path = os.path.join(self.output_dir, files[0])

        table = pcsv.read_csv(Path(csv_path))
        table_list = table.to_pylist()
        self.assertGreaterEqual(len(table_list), 2)
        self.assertCountEqual([{"field": 1}, {"field": 2}], table_list[0:2])

    async def test_runtime_kill_processor_pool(self):
        app = Flow()

        @app.consumer(
            source=PulseWithBacklog(
                [{"field": 1}, {"field": 2}],
                pulse_interval_seconds=1,
                # Set an artificial backlog size to force the consumer to scale up.
                # We set this to a very large value to ensure we scale up to
                # out max capacity.
                backlog_size=1000000,
            ),
            sink=File(file_path=self.output_path, file_format=FileFormat.CSV),
        )
        def process(payload):
            return payload

        runtime_options = RuntimeOptions.default()
        runtime_options.log_level = "DEBUG"
        runtime_options.checkin_frequency_loop_secs = 1
        runtime_options.processor_options["process"] = ProcessorOptions.default()
        # NOTE: We need to set the num_cpus to a small value since pytest limits the
        # number of CPUs available to the test process. (I didnt actually verify this
        # but I think its true)
        runtime_options.processor_options["process"].num_cpus = 0.25
        runtime_options.processor_options[
            "process"
        ].autoscaler_options.autoscale_frequency_secs = 5
        actor = RuntimeActor.remote(
            run_id="test-run", runtime_options=runtime_options, flow_dependencies={}
        )

        await actor.run.remote(
            processor_groups=[ConsumerGroup(processors=[process], group_id="process")],
            serve_port=0,
            serve_host="unused",
            event_subscriber=None,
        )
        # Run for ten seconds to let it scale up.
        pending = await self.run_for_time(actor.run_until_complete.remote(), 10)

        # Grab snapshot to see how many replicas we have scaled up to.
        snapshot = await self.run_with_timeout(actor.snapshot.remote())
        num_replicas = snapshot.processor_groups[0].num_replicas

        # Kill our process pool actor.
        # The let the consumer run for a couple seconds to allow it to be spun
        # up again.
        pool_actor = list_actors(
            filters=[("class_name", "=", "ConsumerProcessorReplicaPoolActor")]
        )[0]
        pid = pool_actor["pid"]
        os.kill(pid, signal.SIGKILL)

        await self.run_for_time(pending, 20)
        snapshot = await self.run_with_timeout(actor.snapshot.remote())
        self.assertGreaterEqual(snapshot.processor_groups[0].num_replicas, num_replicas)

        await self.run_with_timeout(actor.drain.remote(), fail=True)

        self.assertInStderr("process actor unexpectedly died. will restart.")

    async def test_runtime_kill_single_replica(self):
        app = Flow()

        @app.consumer(
            source=PulseWithBacklog(
                [{"field": 1}, {"field": 2}],
                pulse_interval_seconds=1,
                # Set an artificial backlog size to force the consumer to scale up.
                # We set this to a very large value to ensure we scale up to
                # out max capacity.
                backlog_size=1000000,
            ),
            sink=File(file_path=self.output_path, file_format=FileFormat.CSV),
        )
        def process(payload):
            return payload

        runtime_options = RuntimeOptions.default()
        runtime_options.log_level = "DEBUG"
        runtime_options.checkin_loop_frequency_sec = 1
        runtime_options.processor_options["process"] = ProcessorOptions.default()
        # NOTE: We need to set the num_cpus to a small value since pytest limits the
        # number of CPUs available to the test process. (I didnt actually verify this
        # but I think its true)
        runtime_options.processor_options["process"].num_cpus = 0.25
        runtime_options.processor_options[
            "process"
        ].autoscaler_options.autoscale_frequency_secs = 5
        actor = RuntimeActor.remote(
            run_id="test-run",
            runtime_options=runtime_options,
            flow_dependencies={},
        )

        await actor.run.remote(
            processor_groups=[ConsumerGroup(processors=[process], group_id="process")],
            serve_port=0,
            serve_host="unused",
            event_subscriber=None,
        )
        # Run for ten seconds to let it scale up.
        pending = await self.run_for_time(actor.run_until_complete.remote(), 20)

        # Grab snapshot to see how many replicas we have scaled up to.
        snapshot = await self.run_with_timeout(actor.snapshot.remote())
        num_replicas = snapshot.processor_groups[0].num_replicas

        # Kill all replica actors.
        # Then let the consumer run for a couple seconds to allow them to be spun
        # up again.
        replica = list_actors(filters=[("class_name", "=", "PullProcessPushActor")])[0]
        pid = replica["pid"]
        os.kill(pid, signal.SIGKILL)

        await self.run_for_time(pending, 10)
        snapshot = await self.run_with_timeout(actor.snapshot.remote())
        self.assertEqual(num_replicas, snapshot.processor_groups[0].num_replicas)

        await self.run_with_timeout(actor.drain.remote())
        self.assertInStderr("replica actor unexpectedly died. will restart.")

    async def test_runtime_kill_all_replicas(self):
        app = Flow()

        @app.consumer(
            source=PulseWithBacklog(
                [{"field": 1}, {"field": 2}],
                pulse_interval_seconds=1,
                # Set an artificial backlog size to force the consumer to scale up.
                # We set this to a very large value to ensure we scale up to
                # out max capacity.
                backlog_size=1000000,
            ),
            sink=File(file_path=self.output_path, file_format=FileFormat.CSV),
        )
        def process(payload):
            return payload

        runtime_options = RuntimeOptions.default()
        runtime_options.log_level = "DEBUG"
        runtime_options.checkin_loop_frequency_sec = 1
        runtime_options.processor_options["process"] = ProcessorOptions.default()
        # NOTE: We need to set the num_cpus to a small value since pytest limits the
        # number of CPUs available to the test process. (I didnt actually verify this
        # but I think its true)
        runtime_options.processor_options["process"].num_cpus = 0.25
        runtime_options.processor_options[
            "process"
        ].autoscaler_options.autoscale_frequency_secs = 5
        actor = RuntimeActor.remote(
            run_id="test-run",
            runtime_options=runtime_options,
            flow_dependencies={},
        )

        await actor.run.remote(
            processor_groups=[ConsumerGroup(processors=[process], group_id="process")],
            serve_port=0,
            serve_host="unused",
            event_subscriber=None,
        )
        # Run for ten seconds to let it scale up.
        pending = await self.run_for_time(actor.run_until_complete.remote(), 20)

        # Grab snapshot to see how many replicas we have scaled up to.
        snapshot = await self.run_with_timeout(actor.snapshot.remote())

        # Kill all replica actors.
        # Then let the consumer run for a couple seconds to allow them to be spun
        # up again.
        replica_actors = list_actors(
            filters=[("class_name", "=", "PullProcessPushActor")]
        )
        for replica in replica_actors:
            pid = replica["pid"]
            os.kill(pid, signal.SIGKILL)

        await self.run_for_time(pending, 10)
        snapshot = await self.run_with_timeout(actor.snapshot.remote())
        self.assertGreaterEqual(snapshot.processor_groups[0].num_replicas, 1)

        await self.run_with_timeout(actor.drain.remote())
        self.assertInStderr("replica actor unexpectedly died. will restart.")

    async def test_runtime_scales_up(self):
        app = Flow()

        @app.consumer(
            source=PulseWithBacklog(
                [{"field": 1}, {"field": 2}],
                pulse_interval_seconds=1,
                # Set an artificial backlog size to force the consumer to scale up.
                backlog_size=1000,
            ),
            sink=File(file_path=self.output_path, file_format=FileFormat.CSV),
        )
        def process(payload):
            return payload

        runtime_options = RuntimeOptions.default()
        runtime_options.checkin_loop_frequency_sec = 1
        runtime_options.processor_options["process"] = ProcessorOptions.default()
        # NOTE: We need to set the num_cpus to a small value since pytest limits the
        # number of CPUs available to the test process. (I didnt actually verify this
        # but I think its true)
        runtime_options.processor_options["process"].num_cpus = 0.25
        runtime_options.processor_options[
            "process"
        ].autoscaler_options.autoscale_frequency_secs = 5
        actor = RuntimeActor.remote(
            run_id="test-run",
            runtime_options=runtime_options,
            flow_dependencies={},
        )

        await self.run_with_timeout(
            actor.run.remote(
                processor_groups=[
                    ConsumerGroup(processors=[process], group_id="process")
                ],
                serve_port=0,
                serve_host="unused",
                event_subscriber=None,
            )
        )

        await self.run_for_time(actor.run_until_complete.remote(), 15)

        snapshot = await self.run_with_timeout(actor.snapshot.remote())
        self.assertGreaterEqual(snapshot.processor_groups[0].num_replicas, 2)

        await self.run_with_timeout(actor.drain.remote())


if __name__ == "__main__":
    unittest.main()
