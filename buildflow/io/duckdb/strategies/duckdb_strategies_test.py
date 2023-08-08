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


@pytest.mark.usefixtures("event_loop_instance")
class DuckDBStrategiesTest(unittest.TestCase):
    def get_async_result(self, coro):
        """Run a coroutine synchronously."""
        return self.event_loop.run_until_complete(coro)

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

    def test_duckdb_push_base(self):
        # Test new table is created
        data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        self.get_async_result(self.sink.push(data))

        conn = duckdb.connect(self.db, read_only=False)
        got_data = conn.execute(f"SELECT * FROM {self.table}").fetchdf()
        self.assertEqual(got_data.to_dict("records"), data)
        conn.close()

        # Test new rows are appended
        more_data = [{"a": 5, "b": 6}, {"a": 7, "b": 8}]
        self.get_async_result(self.sink.push(more_data))

        conn = duckdb.connect(self.db)
        got_data = conn.execute(f"SELECT * FROM {self.table}").fetchdf()
        self.assertEqual(got_data.to_dict("records"), data + more_data)
        conn.close()

    def test_duckdb_connection_failure(self):
        duckdb_strategies._MAX_CONNECT_TRIES = 2

        # open a connection so we can't connect when pushing
        p = Process(target=open_connection, args=[self.db])
        p.start()
        # Wait for two seconds to ensure the above connection is open
        time.sleep(2)

        data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        with self.assertRaises(ValueError):
            self.get_async_result(self.sink.push(data))

        p.kill()

        self.assertEqual(len(self._caplog.records), 3)

        first_failure = self._caplog.records[0]
        retry_log = self._caplog.records[1]
        final_failure = self._caplog.records[2]

        self.assertEqual(first_failure.levelname, "ERROR")
        self.assertEqual(first_failure.message, "failed to connect to duckdb database")
        self.assertEqual(retry_log.levelname, "WARNING")
        self.assertEqual(
            retry_log.message,
            "can't concurrently write to DuckDB waiting 2 seconds then will try again",
        )
        self.assertEqual(final_failure.levelname, "ERROR")
        self.assertEqual(final_failure.message, "failed to connect to duckdb database")


if __name__ == "__main__":
    unittest.main()
