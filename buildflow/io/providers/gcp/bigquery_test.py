# ruff: noqa
# TODO: remove this ruff disable once tests are enabled again
"""Tests for redis.py"""

from dataclasses import dataclass
import unittest
from unittest import mock

import pytest


@dataclass
class FakeTable:
    project: str
    dataset_id: str
    table_id: str
    num_rows: int


@dataclass
class FakeRow:
    value: int


# NOTE: Async actors don't support local mode / mocks so this really only tests
# the initial setup of the source by calling the source_args() source method.
class BigQueryTest(unittest.TestCase):
    @pytest.mark.skip(reason="TODO: need to port test")
    @mock.patch("google.cloud.bigquery.Client")
    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.bigquery_storage_v1.BigQueryReadClient")
    def test_validate_setup_query_no_temp_dataset(
        self,
        bq_storage_mock: mock.MagicMock,
        auth_mock: mock.MagicMock,
        bq_mock: mock.MagicMock,
    ):
        pass
        # auth_mock.return_value = (None, None)
        # bq_storage_client = bq_storage_mock.return_value
        # bq_client_mock = bq_mock.return_value

        # query = "SELECT * FROM TABLE"

        # source = buildflow.BigQuerySource(query=query, billing_project="tmp")
        # source.setup()
        # source.actor([], None)

        # bq_client_mock.create_dataset.assert_called_once()
        # bq_client_mock.update_dataset.assert_called_once()
        # # This should be called twice, once as a dry run validation, and once
        # # as a real query.
        # self.assertEqual(bq_client_mock.query.call_count, 2)
        # bq_client_mock.get_table.assert_called_once()

        # bq_storage_client.create_read_session.assert_called_once()

    @pytest.mark.skip(reason="TODO: need to port test")
    @mock.patch("google.cloud.bigquery.Client")
    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.bigquery_storage_v1.BigQueryReadClient")
    def test_validate_setup_query_with_temp_dataset(
        self,
        bq_storage_mock: mock.MagicMock,
        auth_mock: mock.MagicMock,
        bq_mock: mock.MagicMock,
    ):
        pass
        # auth_mock.return_value = (None, None)
        # bq_storage_client = bq_storage_mock.return_value

        # query = "SELECT * FROM TABLE"

        # source = buildflow.BigQuerySource(
        #     query=query, temp_dataset="p.ds", billing_project="tmp"
        # )
        # source.actor([], None)

        # bq_client_mock = bq_mock.return_value
        # bq_client_mock.create_dataset.assert_not_called()
        # bq_client_mock.update_dataset.assert_not_called()
        # bq_client_mock.query.assert_called_once_with(query, job_config=mock.ANY)
        # bq_client_mock.get_table.assert_called_once()

        # bq_storage_client.create_read_session.assert_called_once()

    @pytest.mark.skip(reason="TODO: need to port test")
    @mock.patch("google.cloud.bigquery.Client")
    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.bigquery_storage_v1.BigQueryReadClient")
    def test_validate_setup_table(
        self,
        bq_storage_mock: mock.MagicMock,
        auth_mock: mock.MagicMock,
        bq_mock: mock.MagicMock,
    ):
        pass
        # auth_mock.return_value = (None, None)
        # bq_storage_client = bq_storage_mock.return_value

        # bq_client_mock = bq_mock.return_value
        # bq_client_mock.get_table.return_value = FakeTable("p", "d", "t", 10)

        # source = buildflow.BigQuerySource(table_id="p.d.t", billing_project="tmp")
        # source.actor([], None)

        # bq_client_mock.create_dataset.assert_not_called()
        # bq_client_mock.update_dataset.assert_not_called()
        # bq_client_mock.query.assert_not_called()
        # bq_client_mock.get_table.assert_called_once_with("p.d.t")

        # bq_storage_client.create_read_session.assert_called_once_with(
        #     parent="projects/p",
        #     read_session=bigquery_storage_v1.types.ReadSession(
        #         table="projects/p/datasets/d/tables/t",
        #         data_format=bigquery_storage_v1.types.DataFormat.ARROW,
        #     ),
        #     max_stream_count=1000,
        # )

    @pytest.mark.skip(reason="TODO: need to port test")
    @mock.patch("google.cloud.bigquery.Client")
    @mock.patch("google.auth.default")
    def test_bigquery_source_setup_table_id(
        self, auth_mock: mock.MagicMock, bq_mock: mock.MagicMock
    ):
        pass
        # auth_mock.return_value = (None, None)
        # bq = bigquery_io.BigQuerySource(table_id="p.ds.t", billing_project="tmp")

        # bq.setup()

        # bq_mock.return_value.get_table.assert_called_once_with(table="p.ds.t")

    @pytest.mark.skip(reason="TODO: need to port test")
    @mock.patch("google.cloud.bigquery.Client")
    @mock.patch("google.auth.default")
    def test_bigquery_source_setup_query(
        self, auth_mock: mock.MagicMock, bq_mock: mock.MagicMock
    ):
        pass
        # auth_mock.return_value = (None, None)
        # bq = bigquery_io.BigQuerySource(query="query", billing_project="tmp")

        # bq.setup()

        # bq_mock.return_value.query.assert_called_once()

    @pytest.mark.skip(reason="TODO: need to port test")
    @mock.patch("google.cloud.bigquery.Client")
    @mock.patch("google.auth.default")
    def test_bigquery_sink_setup_create_table(
        self, auth_mock: mock.MagicMock, bq_mock: mock.MagicMock
    ):
        pass
        # auth_mock.return_value = (None, None)
        # bq_client_mock = bq_mock.return_value
        # bq_client_mock.get_table.side_effect = exceptions.NotFound("unused")

        # bq = bigquery_io.BigQuerySink(table_id="p.ds.t")

        # @dataclass
        # class Output:
        #     field: int

        # def process() -> Output:
        #     pass

        # bq.setup(process_arg_spec=inspect.getfullargspec(process))

        # bq_client_mock.create_dataset.assert_called_once_with("p.ds", exists_ok=True)

        # bq_client_mock.create_table.assert_called_once()

        # table_call: bigquery.Table = bq_client_mock.create_table.call_args[0][0]
        # self.assertEqual(table_call.project, "p")
        # self.assertEqual(table_call.dataset_id, "ds")
        # self.assertEqual(table_call.table_id, "t")
        # self.assertEqual(
        #     table_call.schema,
        #     [bigquery.SchemaField(name="field", field_type="INTEGER", mode="REQUIRED")],
        # )

    @pytest.mark.skip(reason="TODO: need to port test")
    @mock.patch("google.cloud.bigquery.Client")
    @mock.patch("google.auth.default")
    def test_bigquery_sink_setup_schema_mismatch(
        self, auth_mock: mock.MagicMock, bq_mock: mock.MagicMock
    ):
        pass
        # auth_mock.return_value = (None, None)
        # bq_client_mock = bq_mock.return_value
        # bq_client_mock.get_table.return_value = bigquery.Table(
        #     "p.ds.j",
        #     schema=[
        #         bigquery.SchemaField(name="field", field_type="FLOAT", mode="REQUIRED")
        #     ],
        # )

        # bq = bigquery_io.BigQuerySink(table_id="p.ds.t")

        # @dataclass
        # class Output:
        #     field: int

        # def process() -> Output:
        #     pass

        # with self.assertRaises(ValueError):
        #     bq.setup(process_arg_spec=inspect.getfullargspec(process))

    @pytest.mark.skip(reason="TODO: need to port test")
    @mock.patch("google.cloud.bigquery.Client")
    @mock.patch("google.auth.default")
    def test_bigquery_sink_setup_schema_match(
        self, auth_mock: mock.MagicMock, bq_mock: mock.MagicMock
    ):
        pass
        # auth_mock.return_value = (None, None)
        # bq_client_mock = bq_mock.return_value
        # bq_client_mock.get_table.return_value = bigquery.Table(
        #     "p.ds.j",
        #     schema=[
        #         bigquery.SchemaField(
        #             name="field", field_type="INTEGER", mode="REQUIRED"
        #         )
        #     ],
        # )

        # bq = bigquery_io.BigQuerySink(table_id="p.ds.t")

        # @dataclass
        # class Output:
        #     field: int

        # def process() -> Output:
        #     pass

        # bq.setup(process_arg_spec=inspect.getfullargspec(process))

    @pytest.mark.skip(reason="TODO: need to port test")
    @mock.patch("google.cloud.bigquery.Client")
    @mock.patch("google.auth.default")
    def test_bigquery_sink_setup_schema_no_output_type_not_validated(
        self, auth_mock: mock.MagicMock, bq_mock: mock.MagicMock
    ):
        pass
        # auth_mock.return_value = (None, None)
        # bq_client_mock = bq_mock.return_value
        # bq_client_mock.get_table.return_value = bigquery.Table(
        #     "p.ds.j",
        #     schema=[
        #         bigquery.SchemaField(
        #             name="field", field_type="INTEGER", mode="REQUIRED"
        #         )
        #     ],
        # )

        # bq = bigquery_io.BigQuerySink(table_id="p.ds.t")

        # def process():
        #     pass

        # bq.setup(process_arg_spec=inspect.getfullargspec(process))

    @mock.patch("google.cloud.bigquery.Client")
    @mock.patch("google.auth.default")
    def test_bigquery_sink_setup_schema_no_output_type_value_error(
        self, auth_mock: mock.MagicMock, bq_mock: mock.MagicMock
    ):
        pass
        # auth_mock.return_value = (None, None)
        # bq_client_mock = bq_mock.return_value
        # bq_client_mock.get_table.side_effect = exceptions.NotFound("unused")

        # bq = bigquery_io.BigQuerySink(table_id="p.ds.t")

        # def process():
        #     pass

        # with self.assertRaises(ValueError):
        #     bq.setup(process_arg_spec=inspect.getfullargspec(process))

    @pytest.mark.skip(reason="TODO: need to port test")
    @mock.patch("google.cloud.bigquery.Client")
    @mock.patch("google.auth.default")
    def test_bigquery_sink_setup_create_table_list(
        self, auth_mock: mock.MagicMock, bq_mock: mock.MagicMock
    ):
        pass
        # auth_mock.return_value = (None, None)
        # bq_client_mock = bq_mock.return_value
        # bq_client_mock.get_table.side_effect = exceptions.NotFound("unused")

        # bq = bigquery_io.BigQuerySink(table_id="p.ds.t")

        # @dataclass
        # class Output:
        #     field: int

        # def process() -> List[Output]:
        #     pass

        # bq.setup(process_arg_spec=inspect.getfullargspec(process))

        # bq_client_mock.create_dataset.assert_called_once_with("p.ds", exists_ok=True)

        # bq_client_mock.create_table.assert_called_once()

        # table_call: bigquery.Table = bq_client_mock.create_table.call_args[0][0]
        # self.assertEqual(table_call.project, "p")
        # self.assertEqual(table_call.dataset_id, "ds")
        # self.assertEqual(table_call.table_id, "t")
        # self.assertEqual(
        #     table_call.schema,
        #     [bigquery.SchemaField(name="field", field_type="INTEGER", mode="REQUIRED")],
        # )

    @pytest.mark.skip(reason="TODO: need to port test")
    @mock.patch("google.cloud.bigquery.Client")
    @mock.patch("google.auth.default")
    def test_bigquery_sink_setup_create_table_optional(
        self, auth_mock: mock.MagicMock, bq_mock: mock.MagicMock
    ):
        pass
        # auth_mock.return_value = (None, None)
        # bq_client_mock = bq_mock.return_value
        # bq_client_mock.get_table.side_effect = exceptions.NotFound("unused")

        # bq = bigquery_io.BigQuerySink(table_id="p.ds.t")

        # @dataclass
        # class Output:
        #     field: int

        # def process() -> Optional[Output]:
        #     pass

        # bq.setup(process_arg_spec=inspect.getfullargspec(process))

        # bq_client_mock.create_dataset.assert_called_once_with("p.ds", exists_ok=True)

        # bq_client_mock.create_table.assert_called_once()

        # table_call: bigquery.Table = bq_client_mock.create_table.call_args[0][0]
        # self.assertEqual(table_call.project, "p")
        # self.assertEqual(table_call.dataset_id, "ds")
        # self.assertEqual(table_call.table_id, "t")
        # self.assertEqual(
        #     table_call.schema,
        #     [bigquery.SchemaField(name="field", field_type="INTEGER", mode="REQUIRED")],
        # )

    @pytest.mark.skip(reason="TODO: need to port test")
    @mock.patch("google.cloud.bigquery.Client")
    @mock.patch("google.auth.default")
    def test_bigquery_sink_setup_create_table_iterable(
        self, auth_mock: mock.MagicMock, bq_mock: mock.MagicMock
    ):
        pass
        # auth_mock.return_value = (None, None)
        # bq_client_mock = bq_mock.return_value
        # bq_client_mock.get_table.side_effect = exceptions.NotFound("unused")

        # bq = bigquery_io.BigQuerySink(table_id="p.ds.t")

        # @dataclass
        # class Output:
        #     field: int

        # def process() -> Iterable[Output]:
        #     pass

        # bq.setup(process_arg_spec=inspect.getfullargspec(process))

        # bq_client_mock.create_dataset.assert_called_once_with("p.ds", exists_ok=True)

        # bq_client_mock.create_table.assert_called_once()

        # table_call: bigquery.Table = bq_client_mock.create_table.call_args[0][0]
        # self.assertEqual(table_call.project, "p")
        # self.assertEqual(table_call.dataset_id, "ds")
        # self.assertEqual(table_call.table_id, "t")
        # self.assertEqual(
        #     table_call.schema,
        #     [bigquery.SchemaField(name="field", field_type="INTEGER", mode="REQUIRED")],
        # )

    @pytest.mark.skip(reason="TODO: need to port test")
    def test_source_plan_table_id(self):
        pass
        # expected_plan = NodePlan(
        #     name="",
        #     processors=[
        #         ProcessorPlan(
        #             name="bq_process",
        #             source_resources={
        #                 "source_type": "BigQuerySource",
        #                 "table_id": "p.ds.t",
        #             },
        #             sink_resources=None,
        #         )
        #     ],
        # )
        # app = buildflow.Node()

        # @app.processor(source=bigquery_io.BigQuerySource(table_id="p.ds.t"))
        # def bq_process(elem):
        #     pass

        # plan = app.plan()
        # self.assertEqual(expected_plan, plan)

    @pytest.mark.skip(reason="TODO: need to port test")
    def test_source_plan_query(self):
        pass
        # expected_plan = NodePlan(
        #     name="",
        #     processors=[
        #         ProcessorPlan(
        #             name="bq_process",
        #             source_resources={
        #                 "source_type": "BigQuerySource",
        #                 "temp_dataset": "p.buildflow_temp",
        #             },
        #             sink_resources=None,
        #         )
        #     ],
        # )
        # app = buildflow.Node()

        # @app.processor(
        #     source=bigquery_io.BigQuerySource(
        #         query="select * from t", billing_project="p"
        #     )
        # )
        # def bq_process(elem):
        #     pass

        # plan = app.plan()
        # self.assertEqual(expected_plan, plan)

    @pytest.mark.skip(reason="TODO: need to port test")
    def test_sink_plan_no_schema(self):
        pass
        # expected_plan = NodePlan(
        #     name="",
        #     processors=[
        #         ProcessorPlan(
        #             name="bq_process",
        #             source_resources={
        #                 "source_type": "BigQuerySource",
        #                 "table_id": "p.ds.t1",
        #             },
        #             sink_resources={
        #                 "sink_type": "BigQuerySink",
        #                 "table": {
        #                     "table_id": "p.ds.t2",
        #                     "schema": None,
        #                 },
        #             },
        #         )
        #     ],
        # )
        # app = buildflow.Node()

        # @app.processor(
        #     source=bigquery_io.BigQuerySource(table_id="p.ds.t1"),
        #     sink=bigquery_io.BigQuerySink(table_id="p.ds.t2"),
        # )
        # def bq_process(elem):
        #     pass

        # plan = app.plan()
        # self.assertEqual(expected_plan, plan)

    @pytest.mark.skip(reason="TODO: need to port test")
    def test_sink_plan_schema(self):
        pass
        # expected_plan = NodePlan(
        #     name="",
        #     processors=[
        #         ProcessorPlan(
        #             name="bq_process",
        #             source_resources={
        #                 "source_type": "BigQuerySource",
        #                 "table_id": "p.ds.t1",
        #             },
        #             sink_resources={
        #                 "sink_type": "BigQuerySink",
        #                 "table": {
        #                     "table_id": "p.ds.t2",
        #                     "schema": [
        #                         {"name": "value", "type": "INTEGER", "mode": "REQUIRED"}
        #                     ],
        #                 },
        #             },
        #         )
        #     ],
        # )
        # app = buildflow.Node()

        # @app.processor(
        #     source=bigquery_io.BigQuerySource(table_id="p.ds.t1"),
        #     sink=bigquery_io.BigQuerySink(table_id="p.ds.t2"),
        # )
        # def bq_process(elem) -> FakeRow:
        #     pass

        # plan = app.plan()
        # self.assertEqual(expected_plan, plan)

    @pytest.mark.skip(reason="TODO: need to port test")
    def test_sink_plan_with_bucket(self):
        pass
        # expected_plan = NodePlan(
        #     name="",
        #     processors=[
        #         ProcessorPlan(
        #             name="bq_process",
        #             source_resources={
        #                 "source_type": "BigQuerySource",
        #                 "table_id": "p.ds.t1",
        #             },
        #             sink_resources={
        #                 "sink_type": "BigQuerySink",
        #                 "table": {
        #                     "table_id": "p.ds.t2",
        #                     "schema": None,
        #                 },
        #                 "gcs_bucket": "bucket",
        #             },
        #         )
        #     ],
        # )
        # app = buildflow.Node()

        # @app.processor(
        #     source=bigquery_io.BigQuerySource(table_id="p.ds.t1"),
        #     sink=bigquery_io.BigQuerySink(table_id="p.ds.t2", temp_gcs_bucket="bucket"),
        # )
        # def bq_process(elem):
        #     pass

        # plan = app.plan()
        # self.assertEqual(expected_plan, plan)


if __name__ == "__main__":
    unittest.main()
