import os
import tempfile
import time
import unittest
import uuid
from multiprocessing import Process

import duckdb
import pytest

from buildflow.io.duckdb.strategies import duckdb_strategies


def open_connection(db):
    conn = duckdb.connect(db, read_only=False)
    time.sleep(5)
    conn.close()


class DuckDBStrategiesTest(unittest.IsolatedAsyncioTestCase):
    @pytest.fixture(autouse=True)
    def inject_fixtures(self, caplog):
        self._caplog = caplog

    def setUp(self) -> None:
        # Use in memory table for testing
        self.db = os.path.join(tempfile.gettempdir(), f"{str(uuid.uuid4())}.duck-db")
        self.table = "test_table"
        self.sink = duckdb_strategies.DuckDBSink(
            credentials=None, database=self.db, table=self.table
        )

    def tearDown(self) -> None:
        try:
            os.remove(self.db)
        except FileNotFoundError:
            pass

    async def test_duckdb_push_base(self):
        # Test new table is created
        data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        await self.sink.push(data)

        conn = duckdb.connect(self.db, read_only=False)
        got_data = conn.execute(f"SELECT * FROM {self.table}").fetchdf()
        self.assertEqual(got_data.to_dict("records"), data)
        conn.close()

        # Test new rows are appended
        more_data = [{"a": 5, "b": 6}, {"a": 7, "b": 8}]
        await self.sink.push(more_data)

        conn = duckdb.connect(self.db)
        got_data = conn.execute(f"SELECT * FROM {self.table}").fetchdf()
        self.assertEqual(got_data.to_dict("records"), data + more_data)
        conn.close()

    async def test_duckdb_connection_failure(self):
        duckdb_strategies._MAX_CONNECT_TRIES = 2

        # open a connection so we can't connect when pushing
        p = Process(target=open_connection, args=[self.db])
        p.start()
        # Wait for two seconds to ensure the above connection is open
        time.sleep(2)

        data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        with self.assertRaises(ValueError):
            await self.sink.push(data)

        p.kill()

        found_failure_log = False
        found_retry_log = True
        for log in self._caplog.records:
            if (
                log.levelname == "ERROR"
                and log.message == "failed to connect to duckdb database"
            ):
                found_failure_log = True
            if (
                log.levelname == "WARNING"
                and log.message
                == "can't concurrently write to DuckDB waiting 2 seconds then will try again"  # noqa
            ):
                found_retry_log = True

        self.assertTrue(found_failure_log, "failed to find failure log")
        self.assertTrue(found_retry_log, "failed to find retry log")


if __name__ == "__main__":
    unittest.main()
