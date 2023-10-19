import dataclasses
import datetime
import unittest
from typing import List, Optional

from buildflow.io.snowflake.pulumi.schemas import (
    SnowflakeColumn,
    type_to_snowflake_columns,
)


@dataclasses.dataclass
class Nested:
    int_field: int


@dataclasses.dataclass
class AllTypes:
    int_field: int
    str_field: str
    bool_field: bool
    float_field: float
    date_field: datetime.date
    datetime_field: datetime.datetime
    time_field: datetime.time
    bytes_field: bytes
    list_field: List[int]
    optional_field: Optional[int]
    nested_field: Nested
    optional_list_field: Optional[List[int]]
    optional_nested_field: Optional[Nested]


class SnowflakeSchemasTest(unittest.TestCase):
    def test_all_types_dataclass(self):
        expected_cols = [
            SnowflakeColumn(name="int_field", col_type="INTEGER", nullable=False),
            SnowflakeColumn(name="str_field", col_type="STRING", nullable=False),
            SnowflakeColumn(name="bool_field", col_type="BOOLEAN", nullable=False),
            SnowflakeColumn(name="float_field", col_type="FLOAT", nullable=False),
            SnowflakeColumn(name="date_field", col_type="DATE", nullable=False),
            SnowflakeColumn(
                name="datetime_field", col_type="TIMESTAMP", nullable=False
            ),
            SnowflakeColumn(name="time_field", col_type="TIME", nullable=False),
            SnowflakeColumn(name="bytes_field", col_type="BINARY", nullable=False),
            SnowflakeColumn(name="list_field", col_type="ARRAY", nullable=False),
            SnowflakeColumn(name="optional_field", col_type="INTEGER", nullable=True),
            SnowflakeColumn(name="nested_field", col_type="OBJECT", nullable=False),
            SnowflakeColumn(
                name="optional_list_field", col_type="ARRAY", nullable=True
            ),
            SnowflakeColumn(
                name="optional_nested_field", col_type="OBJECT", nullable=True
            ),
        ]
        cols = type_to_snowflake_columns(AllTypes)

        self.assertEqual(expected_cols, cols)

    def test_not_a_dataclass(self):
        with self.assertRaises(ValueError):
            type_to_snowflake_columns(int)


if __name__ == "__main__":
    unittest.main()
