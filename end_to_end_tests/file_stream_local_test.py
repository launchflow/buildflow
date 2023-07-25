import asyncio
import os
import shutil
import tempfile
from typing import Dict
import unittest

import duckdb
import pytest

import buildflow
from buildflow.core.io.portable.file_change_stream import FileChangeStream
from buildflow.core.io.portable.table import AnalysisTable
from buildflow.core.types.portable_types import FileChangeEvent


@pytest.mark.usefixtures("ray_fix")
@pytest.mark.usefixtures("event_loop_instance")
class FileStreamLocalTest(unittest.TestCase):
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
        self.dir_to_watch = tempfile.mkdtemp()
        self.table = "end_to_end_test"
        self.database = os.path.join(os.getcwd(), "buildflow_managed.duckdb")

    def tearDown(self) -> None:
        shutil.rmtree(self.dir_to_watch)
        os.remove(self.database)

    def test_file_stream_end_to_end(self):
        app = buildflow.Flow()

        @app.pipeline(
            source=FileChangeStream(file_path=self.dir_to_watch),
            sink=AnalysisTable(table_name=self.table),
            num_cpus=0.5,
        )
        def my_pipeline(event: FileChangeEvent) -> Dict[str, str]:
            return event.metadata

        run_coro = app.run(block=False)

        # wait for 10 seconds to let it spin up
        run_coro = self.run_for_time(run_coro, time=20)

        create_path = os.path.join(self.dir_to_watch, "file.txt")
        with open(create_path, "w") as f:
            f.write("hello")

        run_coro = self.run_for_time(run_coro, time=20)

        database = os.path.join(os.getcwd(), "buildflow_managed.duckdb")
        conn = duckdb.connect(database=database, read_only=True)
        got_data = conn.execute(f"SELECT count(*) FROM {self.table}").fetchone()

        self.assertEqual(got_data[0], 1)
        self.get_async_result(app._drain())


if __name__ == "__main__":
    unittest.main()
