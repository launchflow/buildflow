"""Tests for redis.py"""

import dataclasses
import unittest
from unittest import mock

import buildflow as flow
from buildflow.runtime.ray_io import bigquery_io
from google.cloud import bigquery_storage_v1


@dataclasses.dataclass
class FakeTable:
    project: str
    dataset_id: str
    table_id: str
    num_rows: int


# NOTE: Async actors don't support local mode / mocks so this really only tests
# the initial setup of the source by calling the source_args() source method.
class BigQueryTest(unittest.TestCase):

    @mock.patch('google.cloud.bigquery.Client')
    @mock.patch('google.cloud.bigquery_storage_v1.BigQueryReadClient')
    def test_validate_setup_query_no_temp_dataset(
        self,
        bq_storage_mock: mock.MagicMock,
        bq_mock: mock.MagicMock,
    ):

        bq_storage_client = bq_storage_mock.return_value
        bq_client_mock = bq_mock.return_value

        query = 'SELECT * FROM TABLE'

        bigquery_io.BigQuerySourceActor.source_args(
            flow.BigQuery(query=query, billing_project='tmp'), 1)

        bq_client_mock.create_dataset.assert_called_once()
        bq_client_mock.update_dataset.assert_called_once()
        bq_client_mock.query.assert_called_once_with(query,
                                                     job_config=mock.ANY)
        bq_client_mock.get_table.assert_called_once()

        bq_storage_client.create_read_session.assert_called_once()

    @mock.patch('google.cloud.bigquery.Client')
    @mock.patch('google.cloud.bigquery_storage_v1.BigQueryReadClient')
    def test_validate_setup_query_with_temp_dataset(
        self,
        bq_storage_mock: mock.MagicMock,
        bq_mock: mock.MagicMock,
    ):

        bq_storage_client = bq_storage_mock.return_value

        query = 'SELECT * FROM TABLE'

        bigquery_io.BigQuerySourceActor.source_args(
            flow.BigQuery(query=query,
                          temp_dataset='p.ds',
                          billing_project='tmp'), 1)

        bq_client_mock = bq_mock.return_value
        bq_client_mock.create_dataset.assert_not_called()
        bq_client_mock.update_dataset.assert_not_called()
        bq_client_mock.query.assert_called_once_with(query,
                                                     job_config=mock.ANY)
        bq_client_mock.get_table.assert_called_once()

        bq_storage_client.create_read_session.assert_called_once()

    @mock.patch('google.cloud.bigquery.Client')
    @mock.patch('google.cloud.bigquery_storage_v1.BigQueryReadClient')
    def test_validate_setup_table(
        self,
        bq_storage_mock: mock.MagicMock,
        bq_mock: mock.MagicMock,
    ):

        bq_storage_client = bq_storage_mock.return_value

        bq_client_mock = bq_mock.return_value
        bq_client_mock.get_table.return_value = FakeTable('p', 'd', 't', 10)

        bigquery_io.BigQuerySourceActor.source_args(
            flow.BigQuery(table_id='p.d.t', billing_project='tmp'), 1)

        bq_client_mock.create_dataset.assert_not_called()
        bq_client_mock.update_dataset.assert_not_called()
        bq_client_mock.query.assert_not_called()
        bq_client_mock.get_table.assert_called_once_with('p.d.t')

        bq_storage_client.create_read_session.assert_called_once_with(
            parent='projects/p',
            read_session=bigquery_storage_v1.types.ReadSession(
                table='projects/p/datasets/d/tables/t',
                data_format=bigquery_storage_v1.types.DataFormat.ARROW),
            max_stream_count=1)


if __name__ == '__main__':
    unittest.main()
