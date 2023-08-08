import dataclasses
import datetime
from typing import List, Type


@dataclasses.dataclass
class SnowflakeColumn:
    name: str
    col_type: str
    nullable: bool


_PYTYPE_TO_SNOWFLAKE_TYPE = {
    int: "INTEGER",
    str: "STRING",
    float: "FLOAT",
    bool: "BOOLEAN",
    datetime.datetime: "TIMESTAMP",
    datetime.date: "DATE",
    datetime.time: "TIME",
    bytes: "BINARY",
}


def _dataclass_to_snowflake_columns(type_: Type):
    fields = dataclasses.fields(type_)
    columns = []
    for field in fields:
        name = field.name
        is_nullable = False
        field_type = field.type
        if (
            hasattr(field.type, "__args__")
            and len(field.type.__args__) == 2
            and field.type.__args__[-1] is type(None)
        ):  # noqa: E721
            is_nullable = True
            field_type = field.type.__args__[0]
        if hasattr(field_type, "__args__") and field_type.__origin__ is list:
            col_type = "ARRAY"
        elif dataclasses.is_dataclass(field_type):
            col_type = "OBJECT"
        elif field_type in _PYTYPE_TO_SNOWFLAKE_TYPE:
            col_type = _PYTYPE_TO_SNOWFLAKE_TYPE[field_type]
        else:
            raise ValueError(f"Can't convert type: {field_type} to a snowflake type")
        columns.append(SnowflakeColumn(name, col_type, is_nullable))
    return columns


def type_to_snowflake_columns(type_: Type) -> List[SnowflakeColumn]:
    if not dataclasses.is_dataclass(type_):
        raise ValueError(
            "Please specify a dataclass type so we can determine the expected schema"
        )
    return _dataclass_to_snowflake_columns(type_)
