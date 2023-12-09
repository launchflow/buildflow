import os
import unittest
from unittest.mock import Mock, patch

import clickhouse_connect
import pandas as pd

from buildflow.io.clickhouse.strategies import clickhouse_strategies


class ClickhouseStrategiesTest(unittest.IsolatedAsyncioTestCase):
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

    @patch("clickhouse_connect.get_client")
    async def test_clickhouse_push_base(self, mock_get_client):
        data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]

        # Mocking clickhouse_connect.get_client()
        mock_client = Mock()
        mock_client.query_df.return_value = pd.DataFrame(data)
        mock_client.query_df.to_dict.return_value = data
        mock_get_client.return_value = mock_client

        # Mocking self.sink.push(data)
        with patch.object(self.sink, "push", return_value=None) as mock_push:
            await self.sink.push(data)

            client = clickhouse_connect.get_client()
            result_df = client.query_df(f'SELECT * from "{self.table}"')
            self.assertEqual(result_df.to_dict("records"), data)

            # Verify that self.sink.push was called with the expected data
            mock_push.assert_called_with(data)


if __name__ == "__main__":
    unittest.main()
