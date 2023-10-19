import asyncio
import os
import shutil
import tempfile
import unittest
from multiprocessing import Process
from typing import Dict

import duckdb
import pytest
import requests

import buildflow
from buildflow.io.portable.file_change_stream import FileChangeStream
from buildflow.io.portable.table import AnalysisTable
from buildflow.types.portable import FileChangeEvent


def run_flow(dir_to_watch: str, table: str):
    app = buildflow.Flow()

    @app.consumer(
        source=FileChangeStream(file_path=dir_to_watch),
        sink=AnalysisTable(table_name=table),
        num_cpus=0.1,
    )
    def my_consumer(event: FileChangeEvent) -> Dict[str, str]:
        return event.metadata

    app.run(start_runtime_server=True)


@pytest.mark.ray
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
        try:
            os.remove(self.database)
        except FileNotFoundError:
            pass

    def test_file_stream_duckdb_end_to_end_with_runtime_server(self):
        try:
            p = Process(target=run_flow, args=(self.dir_to_watch, self.table))
            p.start()

            # wait for 20 seconds to let it spin up
            self.get_async_result(asyncio.sleep(40))

            create_path = os.path.join(self.dir_to_watch, "file.txt")
            with open(create_path, "w") as f:
                f.write("hello")

            self.get_async_result(asyncio.sleep(20))

            database = os.path.join(os.getcwd(), "buildflow_managed.duckdb")
            conn = duckdb.connect(database=database, read_only=True)
            got_data = conn.execute(f"SELECT count(*) FROM {self.table}").fetchone()

            self.assertEqual(got_data[0], 1)
            response = requests.get(
                "http://127.0.0.1:9653/runtime/snapshot", timeout=10
            )
            response.raise_for_status()

            self.assertEqual(response.json()["status"], "RUNNING")

            response = requests.post("http://127.0.0.1:9653/runtime/drain", timeout=10)
            response.raise_for_status()
        finally:
            p.join(timeout=20)
            if p.is_alive():
                p.kill()
                p.join()


if __name__ == "__main__":
    unittest.main()
