from dataclasses import dataclass
import dataclasses
from datetime import date, datetime, time
from typing import Optional, List, Union
import unittest

from google.cloud import bigquery

from buildflow.io.providers.schemas import bigquery_schemas as bq_schemas


class BigqueryTest(unittest.TestCase):
    def test_primitive_types(self):
        @dataclass
        class Schema:
            int_field: int
            str_field: str
            float_field: float
            timestamp: datetime
            bytes_field: bytes
            bool_field: bool
            date_field: date
            time_field: time

        bq_schema = bq_schemas.dataclass_fields_to_bq_schema(dataclasses.fields(Schema))

        expected_schema = [
            bigquery.SchemaField(
                name="int_field",
                field_type="INTEGER",
                mode="REQUIRED",
            ),
            bigquery.SchemaField(
                name="str_field",
                field_type="STRING",
                mode="REQUIRED",
            ),
            bigquery.SchemaField(
                name="float_field",
                field_type="FLOAT",
                mode="REQUIRED",
            ),
            bigquery.SchemaField(
                name="timestamp",
                field_type="TIMESTAMP",
                mode="REQUIRED",
            ),
            bigquery.SchemaField(
                name="bytes_field",
                field_type="BYTES",
                mode="REQUIRED",
            ),
            bigquery.SchemaField(
                name="bool_field",
                field_type="BOOL",
                mode="REQUIRED",
            ),
            bigquery.SchemaField(
                name="date_field",
                field_type="DATE",
                mode="REQUIRED",
            ),
            bigquery.SchemaField(
                name="time_field",
                field_type="TIME",
                mode="REQUIRED",
            ),
        ]

        self.assertEqual(bq_schema, expected_schema)

    def test_repeated(self):
        @dataclass
        class Schema:
            int_field: List[int]

        expected_schema = [
            bigquery.SchemaField(
                name="int_field",
                field_type="INTEGER",
                mode="REPEATED",
            )
        ]

        bq_schema = bq_schemas.dataclass_fields_to_bq_schema(dataclasses.fields(Schema))
        self.assertEqual(bq_schema, expected_schema)

    def test_optional(self):
        @dataclass
        class Schema:
            int_field: Optional[int]

        expected_schema = [
            bigquery.SchemaField(
                name="int_field",
                field_type="INTEGER",
                mode="NULLABLE",
            )
        ]

        bq_schema = bq_schemas.dataclass_fields_to_bq_schema(dataclasses.fields(Schema))
        self.assertEqual(bq_schema, expected_schema)

    def test_record(self):
        @dataclass
        class Nested:
            val: int

        @dataclass
        class Schema:
            opt_nested: Optional[Nested]
            req_nested: Nested
            list_nested: List[Nested]

        expected_schema = [
            bigquery.SchemaField(
                name="opt_nested",
                field_type="RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField(
                        name="val", field_type="INTEGER", mode="REQUIRED", fields=[]
                    )
                ],
            ),
            bigquery.SchemaField(
                name="req_nested",
                field_type="RECORD",
                mode="REQUIRED",
                fields=[
                    bigquery.SchemaField(
                        name="val", field_type="INTEGER", mode="REQUIRED", fields=[]
                    )
                ],
            ),
            bigquery.SchemaField(
                name="list_nested",
                field_type="RECORD",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField(
                        name="val", field_type="INTEGER", mode="REQUIRED", fields=[]
                    )
                ],
            ),
        ]

        bq_schema = bq_schemas.dataclass_fields_to_bq_schema(dataclasses.fields(Schema))
        self.assertEqual(bq_schema, expected_schema)

    def test_unknown_type(self):
        @dataclass
        class Schema:
            int_field: Union[int, float]

        with self.assertRaises(ValueError):
            bq_schemas.dataclass_fields_to_bq_schema(dataclasses.fields(Schema))


if __name__ == "__main__":
    unittest.main()
