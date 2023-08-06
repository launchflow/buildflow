import asyncio
import os
import signal
import tempfile
import time
import unittest
from pathlib import Path

import pyarrow.csv as pcsv
import pytest
from ray.util.state import list_actors

from buildflow.core.app.flow import Flow
from buildflow.core.app.runtime.actors.runtime import RuntimeActor
from buildflow.core.options import ProcessorOptions, RuntimeOptions
from buildflow.io.local.file import File
from buildflow.io.local.pulse import Pulse
from buildflow.io.local.testing.pulse_with_backlog import PulseWithBacklog
from buildflow.types.portable import FileFormat


@pytest.mark.usefixtures("ray_fix")
@pytest.mark.usefixtures("event_loop_instance")
class RunTimeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.output_path = tempfile.mkstemp(suffix=".csv")[1]

    def tearDown(self) -> None:
        os.remove(self.output_path)

    def run_with_timeout(self, coro, timeout: int = 5):
        """Run a coroutine synchronously."""
        try:
            return self.event_loop.run_until_complete(
                asyncio.wait_for(coro, timeout=timeout)
            )
        except asyncio.TimeoutError:
            return

    def run_for_time(self, coro, time: int = 5):
        async def wait_wrapper():
            completed, pending = await asyncio.wait(
                [coro], timeout=time, return_when="FIRST_EXCEPTION"
            )
            if completed:
                # This general should only happen when there was an exception so
                # we want to raise it to make the test failure more obvious.
                completed.pop().result()
            if pending:
                return pending.pop()

        return self.event_loop.run_until_complete(wait_wrapper())

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
        runtime_options.processor_options["process"].num_cpus = 0.5
        actor = RuntimeActor.remote(run_id="test-run", runtime_options=runtime_options)

        actor.run.remote(processors=[process])

        time.sleep(15)

        self.run_with_timeout(actor.drain.remote())

        table = pcsv.read_csv(Path(self.output_path))
        table_list = table.to_pylist()
        self.assertGreaterEqual(len(table_list), 2)
        self.assertCountEqual([{"field": 1}, {"field": 2}], table_list[0:2])

    def test_runtime_kill_processor_pool(self):
        app = Flow()

        @app.pipeline(
            source=PulseWithBacklog(
                [{"field": 1}, {"field": 2}],
                pulse_interval_seconds=1,
                # Set an artificial backlog size to force the pipeline to scale up.
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
        runtime_options.autoscaler_options.autoscale_frequency_secs = 5
        runtime_options.processor_options["process"] = ProcessorOptions.default()
        # NOTE: We need to set the num_cpus to a small value since pytest limits the
        # number of CPUs available to the test process. (I didnt actually verify this
        # but I think its true)
        runtime_options.processor_options["process"].num_cpus = 0.25
        actor = RuntimeActor.remote(
            run_id="test-run",
            runtime_options=runtime_options,
        )

        self.run_with_timeout(actor.run.remote(processors=[process]))
        # Run for ten seconds to let it scale up.
        pending = self.run_for_time(actor.run_until_complete.remote(), 10)

        # Grab snapshot to see how many replicas we have scaled up to.
        snapshot = self.run_with_timeout(actor.snapshot.remote())
        num_replicas = snapshot.processors[0].num_replicas

        # Kill our process pool actor.
        # The let the pipeline run for a couple seconds to allow it to be spun
        # up again.
        pool_actor = list_actors(
            filters=[("class_name", "=", "PipelineProcessorReplicaPoolActor")]
        )[0]
        pid = pool_actor["pid"]
        os.kill(pid, signal.SIGKILL)

        self.run_for_time(pending, 20)
        snapshot = self.run_with_timeout(actor.snapshot.remote())
        self.assertEqual(num_replicas, snapshot.processors[0].num_replicas)

        self.run_with_timeout(actor.drain.remote())

        self.assertInStderr("process actor unexpectedly died. will restart.")

    def test_runtime_kill_single_replica(self):
        app = Flow()

        @app.pipeline(
            source=PulseWithBacklog(
                [{"field": 1}, {"field": 2}],
                pulse_interval_seconds=1,
                # Set an artificial backlog size to force the pipeline to scale up.
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
        runtime_options.autoscaler_options.autoscale_frequency_secs = 5
        runtime_options.processor_options["process"] = ProcessorOptions.default()
        # NOTE: We need to set the num_cpus to a small value since pytest limits the
        # number of CPUs available to the test process. (I didnt actually verify this
        # but I think its true)
        runtime_options.processor_options["process"].num_cpus = 0.25
        actor = RuntimeActor.remote(
            run_id="test-run",
            runtime_options=runtime_options,
        )

        self.run_with_timeout(actor.run.remote(processors=[process]))
        # Run for ten seconds to let it scale up.
        pending = self.run_for_time(actor.run_until_complete.remote(), 10)

        # Grab snapshot to see how many replicas we have scaled up to.
        snapshot = self.run_with_timeout(actor.snapshot.remote())
        num_replicas = snapshot.processors[0].num_replicas

        # Kill all replica actors.
        # Then let the pipeline run for a couple seconds to allow them to be spun
        # up again.
        replica = list_actors(filters=[("class_name", "=", "PullProcessPushActor")])[0]
        pid = replica["pid"]
        os.kill(pid, signal.SIGKILL)

        self.run_for_time(pending, 10)
        snapshot = self.run_with_timeout(actor.snapshot.remote())
        self.assertEqual(num_replicas, snapshot.processors[0].num_replicas)

        self.run_with_timeout(actor.drain.remote())
        self.assertInStderr("replica actor unexpectedly died. will restart.")

    def test_runtime_kill_all_replicas(self):
        app = Flow()

        @app.pipeline(
            source=PulseWithBacklog(
                [{"field": 1}, {"field": 2}],
                pulse_interval_seconds=1,
                # Set an artificial backlog size to force the pipeline to scale up.
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
        runtime_options.autoscaler_options.autoscale_frequency_secs = 5
        runtime_options.processor_options["process"] = ProcessorOptions.default()
        # NOTE: We need to set the num_cpus to a small value since pytest limits the
        # number of CPUs available to the test process. (I didnt actually verify this
        # but I think its true)
        runtime_options.processor_options["process"].num_cpus = 0.25
        actor = RuntimeActor.remote(
            run_id="test-run",
            runtime_options=runtime_options,
        )

        self.run_with_timeout(actor.run.remote(processors=[process]))
        # Run for ten seconds to let it scale up.
        pending = self.run_for_time(actor.run_until_complete.remote(), 10)

        # Grab snapshot to see how many replicas we have scaled up to.
        snapshot = self.run_with_timeout(actor.snapshot.remote())

        # Kill all replica actors.
        # Then let the pipeline run for a couple seconds to allow them to be spun
        # up again.
        replica_actors = list_actors(
            filters=[("class_name", "=", "PullProcessPushActor")]
        )
        for replica in replica_actors:
            pid = replica["pid"]
            os.kill(pid, signal.SIGKILL)

        self.run_for_time(pending, 10)
        snapshot = self.run_with_timeout(actor.snapshot.remote())
        self.assertGreaterEqual(snapshot.processors[0].num_replicas, 1)

        self.run_with_timeout(actor.drain.remote())
        self.assertInStderr("replica actor unexpectedly died. will restart.")

    def test_runtime_scales_up(self):
        app = Flow()

        @app.pipeline(
            source=PulseWithBacklog(
                [{"field": 1}, {"field": 2}],
                pulse_interval_seconds=1,
                # Set an artificial backlog size to force the pipeline to scale up.
                backlog_size=1000,
            ),
            sink=File(file_path=self.output_path, file_format=FileFormat.CSV),
        )
        def process(payload):
            return payload

        runtime_options = RuntimeOptions.default()
        runtime_options.checkin_loop_frequency_sec = 1
        runtime_options.autoscaler_options.autoscale_frequency_secs = 1
        runtime_options.processor_options["process"] = ProcessorOptions.default()
        # NOTE: We need to set the num_cpus to a small value since pytest limits the
        # number of CPUs available to the test process. (I didnt actually verify this
        # but I think its true)
        runtime_options.processor_options["process"].num_cpus = 0.25
        actor = RuntimeActor.remote(run_id="test-run", runtime_options=runtime_options)

        self.run_with_timeout(actor.run.remote(processors=[process]))

        self.run_for_time(actor.run_until_complete.remote(), 15)

        snapshot = self.run_with_timeout(actor.snapshot.remote())

        self.run_with_timeout(actor.drain.remote())

        self.assertGreaterEqual(snapshot.processors[0].num_replicas, 2)


if __name__ == "__main__":
    unittest.main()
