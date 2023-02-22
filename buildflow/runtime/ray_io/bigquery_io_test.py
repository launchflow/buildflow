"""Tests for redis.py"""

import dataclasses
import os
import tempfile
import unittest
from unittest import mock

from google.cloud import bigquery_storage_v1
import ray

import buildflow as flow


@dataclasses.dataclass
class FakeTable:
    project: str
    dataset_id: str
    table_id: str
    num_rows: int


# NOTE: Async actors don't support local mode / mocks so this really only tests
# the initial setup of the source.
class BigQueryTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=1, num_gpus=0, local_mode=True)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def setUp(self):
        _, self.temp_file = tempfile.mkstemp()

    def tearDown(self):
        os.remove(self.temp_file)

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

        @flow.processor(
            input_ref=flow.BigQuery(query=query),
            output_ref=flow.BigQuery(table_id='project.table.dataset'))
        def pass_through_fn(elem):
            return elem

        flow.run()

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

        @flow.processor(
            input_ref=flow.BigQuery(query=query, temporary_dataset='p.ds'),
            output_ref=flow.BigQuery(table_id='project.table.dataset'))
        def pass_through_fn(elem):
            return elem

        flow.run()

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

        @flow.processor(
            input_ref=flow.BigQuery(table_id='p.d.t'),
            output_ref=flow.BigQuery(table_id='project.table.dataset'))
        def pass_through_fn(elem):
            return elem

        flow.run()

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
