import asyncio
import dataclasses
import os
import shutil
import tempfile
import unittest

import duckdb
import pytest
import requests

import buildflow
from buildflow.core.options.flow_options import FlowOptions
from buildflow.dependencies.base import Scope, dependency
from buildflow.io.local import File
from buildflow.io.portable.table import AnalysisTable
from buildflow.types.portable import FileFormat


@dataclasses.dataclass
class InputRequest:
    val: int


@dataclasses.dataclass
class OutputResponse:
    val: int


@pytest.mark.usefixtures("ray")
@pytest.mark.usefixtures("event_loop_instance")
class CollectorLocalTest(unittest.TestCase):
    def get_async_result(self, coro):
        """Run a coroutine synchronously."""
        return self.event_loop.run_until_complete(coro)

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

    def setUp(self) -> None:
        self.table = "end_to_end_test"
        self.database = os.path.join(os.getcwd(), "buildflow_managed.duckdb")

    def tearDown(self) -> None:
        try:
            os.remove(self.database)
        except FileNotFoundError:
            pass

    def test_collector_duckdb_end_to_end(self):
        app = buildflow.Flow()

        @app.collector(
            route="/test",
            method="POST",
            sink=AnalysisTable(table_name=self.table),
            num_cpus=0.5,
        )
        def my_collector(input: InputRequest) -> OutputResponse:
            return OutputResponse(input.val + 1)

        run_coro = app.run(block=False)

        # wait for 20 seconds to let it spin up
        run_coro = self.run_for_time(run_coro, time=20)

        response = requests.post(
            "http://0.0.0.0:8000/test", json={"val": 1}, timeout=10
        )
        response.raise_for_status()

        database = os.path.join(os.getcwd(), "buildflow_managed.duckdb")
        conn = duckdb.connect(database=database, read_only=True)
        got_data = conn.execute(f"SELECT count(*) FROM {self.table}").fetchone()

        self.assertEqual(got_data[0], 1)
        self.get_async_result(app._drain())

    def test_collector_file_end_to_end(self):
        output_dir = tempfile.mkdtemp()
        try:
            app = buildflow.Flow()

            @app.collector(
                route="/test",
                method="POST",
                sink=File(
                    os.path.join(output_dir, "file.csv"), file_format=FileFormat.CSV
                ),
                num_cpus=0.5,
            )
            def my_collector(input: InputRequest) -> OutputResponse:
                return OutputResponse(input.val + 1)

            run_coro = app.run(block=False)

            # wait for 20 seconds to let it spin up
            run_coro = self.run_for_time(run_coro, time=20)

            response = requests.post(
                "http://localhost:8000/test", json={"val": 1}, timeout=10
            )
            response.raise_for_status()

            run_coro = self.run_for_time(run_coro, time=20)

            output_files = os.listdir(output_dir)
            self.assertEqual(len(output_files), 1)
            output_file = os.path.join(output_dir, output_files[0])

            with open(output_file, "r") as f:
                lines = f.readlines()
                self.assertEqual(len(lines), 2)
                self.assertEqual(lines[0], '"val"\n')
                self.assertEqual(lines[1], "2\n")

            self.get_async_result(app._drain())
        finally:
            shutil.rmtree(output_dir)

    def test_collector_file_end_to_end_class(self):
        output_dir = tempfile.mkdtemp()
        try:
            app = buildflow.Flow()

            @app.collector(
                route="/test",
                method="POST",
                sink=File(
                    os.path.join(output_dir, "file.csv"), file_format=FileFormat.CSV
                ),
                num_cpus=0.5,
            )
            class Collector:
                def setup(self):
                    self.add_val = 10

                def get_add_val(self):
                    return self.add_val

                def process(self, input: InputRequest) -> OutputResponse:
                    return OutputResponse(input.val + self.get_add_val())

            run_coro = app.run(block=False)

            # wait for 20 seconds to let it spin up
            run_coro = self.run_for_time(run_coro, time=20)

            response = requests.post(
                "http://localhost:8000/test", json={"val": 1}, timeout=10
            )
            response.raise_for_status()

            run_coro = self.run_for_time(run_coro, time=20)

            output_files = os.listdir(output_dir)
            self.assertEqual(len(output_files), 1)
            output_file = os.path.join(output_dir, output_files[0])

            with open(output_file, "r") as f:
                lines = f.readlines()
                self.assertEqual(len(lines), 2)
                self.assertEqual(lines[0], '"val"\n')
                self.assertEqual(lines[1], "11\n")

            self.get_async_result(app._drain())
        finally:
            shutil.rmtree(output_dir)

    def test_collector_file_multi_output(self):
        output_dir = tempfile.mkdtemp()
        try:
            app = buildflow.Flow()

            @app.collector(
                route="/test",
                method="POST",
                sink=File(
                    os.path.join(output_dir, "file.csv"), file_format=FileFormat.CSV
                ),
                num_cpus=0.5,
            )
            def my_collector(input: InputRequest) -> OutputResponse:
                return [OutputResponse(input.val + 1), OutputResponse(input.val + 2)]

            run_coro = app.run(block=False)

            # wait for 20 seconds to let it spin up
            run_coro = self.run_for_time(run_coro, time=20)

            response = requests.post(
                "http://localhost:8000/test", json={"val": 1}, timeout=10
            )
            response.raise_for_status()

            run_coro = self.run_for_time(run_coro, time=20)

            output_files = os.listdir(output_dir)
            self.assertEqual(len(output_files), 1)
            output_file = os.path.join(output_dir, output_files[0])

            with open(output_file, "r") as f:
                lines = f.readlines()
                self.assertEqual(len(lines), 3)
                self.assertEqual(lines, ['"val"\n', "2\n", "3\n"])

            self.get_async_result(app._drain())
        finally:
            shutil.rmtree(output_dir)

    def test_collector_duckdb_end_to_end_unattached(self):
        app = buildflow.Flow()

        @buildflow.collector(
            route="/test",
            method="POST",
            sink=AnalysisTable(table_name=self.table),
            num_cpus=0.5,
        )
        def my_collector(input: InputRequest) -> OutputResponse:
            return OutputResponse(input.val + 1)

        app.add_collector(my_collector)

        run_coro = app.run(block=False)

        # wait for 20 seconds to let it spin up
        run_coro = self.run_for_time(run_coro, time=20)

        response = requests.post(
            "http://0.0.0.0:8000/test", json={"val": 1}, timeout=10
        )
        response.raise_for_status()

        database = os.path.join(os.getcwd(), "buildflow_managed.duckdb")
        conn = duckdb.connect(database=database, read_only=True)
        got_data = conn.execute(f"SELECT count(*) FROM {self.table}").fetchone()

        self.assertEqual(got_data[0], 1)
        self.get_async_result(app._drain())

    def test_collector_with_dependencies(self):
        output_dir = tempfile.mkdtemp()

        @dependency(scope=Scope.NO_SCOPE)
        class NoScope:
            def __init__(self):
                self.val = 1

        @dependency(scope=Scope.GLOBAL)
        class GlobalScope:
            def __init__(self, no: NoScope):
                self.val = 2
                self.no = no

        @dependency(scope=Scope.REPLICA)
        class ReplicaScope:
            def __init__(self, global_: GlobalScope):
                self.val = 3
                self.global_ = global_

        @dependency(scope=Scope.PROCESS)
        class ProcessScope:
            def __init__(self, replica: ReplicaScope):
                self.val = 4
                self.replica = replica

        try:
            app = buildflow.Flow(flow_options=FlowOptions(runtime_log_level="DEBUG"))

            @app.collector(
                route="/test",
                method="POST",
                sink=File(
                    os.path.join(output_dir, "file.csv"), file_format=FileFormat.CSV
                ),
                num_cpus=0.5,
            )
            def my_collector(
                input: InputRequest,
                no: NoScope,
                global_: GlobalScope,
                replica: ReplicaScope,
                process: ProcessScope,
            ) -> OutputResponse:
                if id(process.replica) != id(replica):
                    raise Exception("Replica scope not the same")
                if id(replica.global_) != id(global_):
                    raise Exception("Global scope not the same")
                if id(global_.no) == id(no):
                    raise Exception("No scope was the same")
                return OutputResponse(
                    input.val + no.val + global_.val + replica.val + process.val
                )

            run_coro = app.run(block=False)

            # wait for 20 seconds to let it spin up
            run_coro = self.run_for_time(run_coro, time=20)

            response = requests.post(
                "http://localhost:8000/test", json={"val": 1}, timeout=10
            )
            response.raise_for_status()

            run_coro = self.run_for_time(run_coro, time=20)

            output_files = os.listdir(output_dir)
            self.assertEqual(len(output_files), 1)
            output_file = os.path.join(output_dir, output_files[0])

            with open(output_file, "r") as f:
                lines = f.readlines()
                self.assertEqual(len(lines), 2)
                self.assertEqual(lines[0], '"val"\n')
                self.assertEqual(lines[1], "11\n")

            self.get_async_result(app._drain())
        finally:
            shutil.rmtree(output_dir)


if __name__ == "__main__":
    unittest.main()
