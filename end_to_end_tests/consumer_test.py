import asyncio
import os
import shutil
import tempfile
import unittest
from typing import Dict

import duckdb
import pytest

import buildflow
from buildflow.dependencies.base import Scope, dependency
from buildflow.io.local import File
from buildflow.io.portable.file_change_stream import FileChangeStream
from buildflow.io.portable.table import AnalysisTable
from buildflow.types.portable import FileChangeEvent, FileFormat


@pytest.mark.usefixtures("ray")
class FileStreamLocalTest(unittest.IsolatedAsyncioTestCase):
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

    async def run_with_timeout(self, coro, timeout: int = 5, fail: bool = False):
        """Run a coroutine synchronously."""
        try:
            return await asyncio.wait_for(coro, timeout=timeout)
        except asyncio.TimeoutError:
            if fail:
                raise
            return

    def setUp(self) -> None:
        self.dir_to_watch = tempfile.mkdtemp()
        self.table = "end_to_end_test"
        self.database = os.path.join(os.getcwd(), "buildflow_managed.duckdb")

    def tearDown(self) -> None:
        shutil.rmtree(self.dir_to_watch)
        try:
            os.remove(self.database)
        except FileNotFoundError:
            pass

    async def test_file_stream_duckdb_end_to_end(self):
        app = buildflow.Flow()

        @app.consumer(
            source=FileChangeStream(file_path=self.dir_to_watch),
            sink=AnalysisTable(table_name=self.table),
            num_cpus=0.5,
        )
        def my_consumer(event: FileChangeEvent) -> Dict[str, str]:
            return event.metadata

        run_coro = app.run(block=False)

        # wait for 20 seconds to let it spin up
        run_coro = await self.run_for_time(run_coro, time=20)

        create_path = os.path.join(self.dir_to_watch, "file.txt")
        with open(create_path, "w") as f:
            f.write("hello")

        run_coro = await self.run_for_time(run_coro, time=20)

        database = os.path.join(os.getcwd(), "buildflow_managed.duckdb")
        conn = duckdb.connect(database=database, read_only=True)
        got_data = conn.execute(f"SELECT count(*) FROM {self.table}").fetchone()

        self.assertEqual(got_data[0], 1)
        await self.run_with_timeout(app._drain())

    async def test_file_stream_file_end_to_end(self):
        output_dir = tempfile.mkdtemp()
        try:
            app = buildflow.Flow()

            @app.consumer(
                source=FileChangeStream(file_path=self.dir_to_watch),
                sink=File(
                    os.path.join(output_dir, "file.csv"), file_format=FileFormat.CSV
                ),
                num_cpus=0.5,
            )
            def my_consumer(event: FileChangeEvent) -> Dict[str, str]:
                return {"content": event.blob.decode()}

            run_coro = app.run(block=False)

            # wait for 20 seconds to let it spin up
            run_coro = await self.run_for_time(run_coro, time=20)

            create_path = os.path.join(self.dir_to_watch, "hello.txt")
            with open(create_path, "w") as f:
                f.write("hello")

            create_path = os.path.join(self.dir_to_watch, "world.txt")
            with open(create_path, "w") as f:
                f.write("world")

            run_coro = await self.run_for_time(run_coro, time=20)

            output_files = os.listdir(output_dir)
            self.assertEqual(len(output_files), 1)
            output_file = os.path.join(output_dir, output_files[0])

            with open(output_file, "r") as f:
                lines = f.readlines()
                # NOTE: for some reason these come out of order sometimes.
                # I think it's due to the local file system notifications
                # being delivered out of order.
                lines.sort()
                self.assertEqual(len(lines), 3)
                self.assertEqual(lines[0], '"content"\n')
                self.assertEqual(lines[1], '"hello"\n')
                self.assertEqual(lines[2], '"world"\n')

            await self.run_with_timeout(app._drain())
        finally:
            shutil.rmtree(output_dir)

    async def test_file_stream_duckdb_end_to_end_unattached(self):
        app = buildflow.Flow()

        @buildflow.consumer(
            source=FileChangeStream(file_path=self.dir_to_watch),
            sink=AnalysisTable(table_name=self.table),
            num_cpus=0.5,
        )
        def my_consumer(event: FileChangeEvent) -> Dict[str, str]:
            return event.metadata

        app.add_consumer(my_consumer)

        run_coro = app.run(block=False)

        # wait for 20 seconds to let it spin up
        run_coro = await self.run_for_time(run_coro, time=20)

        create_path = os.path.join(self.dir_to_watch, "file.txt")
        with open(create_path, "w") as f:
            f.write("hello")

        run_coro = await self.run_for_time(run_coro, time=20)

        database = os.path.join(os.getcwd(), "buildflow_managed.duckdb")
        conn = duckdb.connect(database=database, read_only=True)
        got_data = conn.execute(f"SELECT count(*) FROM {self.table}").fetchone()

        self.assertEqual(got_data[0], 1)
        await self.run_with_timeout(app._drain())

    async def test_file_stream_duckdb_dependencies(self):
        app = buildflow.Flow()

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

        @app.consumer(
            source=FileChangeStream(file_path=self.dir_to_watch),
            sink=AnalysisTable(table_name=self.table),
            num_cpus=0.5,
        )
        def my_consumer(
            event: FileChangeEvent,
            no: NoScope,
            global_: GlobalScope,
            replica: ReplicaScope,
            process: ProcessScope,
        ) -> Dict[str, int]:
            if id(process.replica) != id(replica):
                raise Exception("Replica scope not the same")
            if id(replica.global_) != id(global_):
                raise Exception("Global scope not the same")
            if id(global_.no) == id(no):
                raise Exception("No scope was the same")
            return {"val": no.val + global_.val + replica.val + process.val}

        run_coro = app.run(block=False)

        # wait for 20 seconds to let it spin up
        run_coro = await self.run_for_time(run_coro, time=20)

        create_path = os.path.join(self.dir_to_watch, "file.txt")
        with open(create_path, "w") as f:
            f.write("hello")

        run_coro = await self.run_for_time(run_coro, time=20)

        database = os.path.join(os.getcwd(), "buildflow_managed.duckdb")
        conn = duckdb.connect(database=database, read_only=True)
        got_data = conn.execute(f"SELECT * FROM {self.table}").fetchone()

        self.assertEqual(got_data[0], 10)
        await self.run_with_timeout(app._drain())


if __name__ == "__main__":
    unittest.main()
