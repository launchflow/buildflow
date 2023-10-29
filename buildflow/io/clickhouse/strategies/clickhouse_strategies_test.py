import os
import unittest

import clickhouse_connect
import pytest

from buildflow.io.clickhouse.strategies import clickhouse_strategies


@pytest.mark.usefixtures("event_loop_instance")
class ClickhouseStrategiesTest(unittest.TestCase):
    def get_async_result(self, coro):
        """Run a coroutine synchronously."""
        return self.event_loop.run_until_complete(coro)

    @pytest.fixture(autouse=True)
    def inject_fixtures(self, caplog):
        self._caplog = caplog

    def tearDown(self) -> None:
        try:
            os.remove(self.db)
        except FileNotFoundError:
            pass

    def setUp(self) -> None:
        self.host = "localhost"
        self.username = "default"
        self.password = ""
        self.db = "default"
        self.table = "default"
        self.sink = clickhouse_strategies.ClickhouseSink(
            credentials=None,
            host=self.host,
            username=self.username,
            password=self.password,
            database=self.db,
            table=self.table,
        )

    def test_clickhouse_push_base(self):
        data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        self.get_async_result(self.sink.push(data))

        client = clickhouse_connect.get_client()
        result_df = client.query_df(f'SELECT * from "{self.table}"')
        self.assertEqual(result_df.to_dict("records"), data)

        # Test new rows are appended
        more_data = [{"a": 5, "b": 6}, {"a": 7, "b": 8}]
        self.get_async_result(self.sink.push(more_data))
        self.assertEqual(result_df.to_dict("records"), data + more_data)


if __name__ == "__main__":
    unittest.main()
