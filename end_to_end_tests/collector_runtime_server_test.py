import asyncio
import dataclasses
import os
import unittest
from multiprocessing import Process

import duckdb
import pytest
import requests

import buildflow
from buildflow.io.portable.table import AnalysisTable


def run_flow(table: str):
    app = buildflow.Flow()

    @app.collector(
        route="/test",
        method="POST",
        sink=AnalysisTable(table_name=table),
        num_cpus=0.1,
    )
    def my_collector(input: InputRequest) -> OutputResponse:
        return OutputResponse(input.val + 1)

    app.run(start_runtime_server=True)


@dataclasses.dataclass
class InputRequest:
    val: int


@dataclasses.dataclass
class OutputResponse:
    val: int


@pytest.mark.ray
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

    def test_collector_duckdb_end_to_end_with_runtime_server(self):
        try:
            p = Process(target=run_flow, args=(self.table,))
            p.start()

            # wait for 20 seconds to let it spin up
            self.get_async_result(asyncio.sleep(40))

            response = requests.post(
                "http://0.0.0.0:8000/test", json={"val": 1}, timeout=10
            )
            response.raise_for_status()

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
