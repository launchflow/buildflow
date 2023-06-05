"""Utilities for working with BigQuery schemas."""

import dataclasses
import datetime
import json
from typing import List, Type

from google.cloud import bigquery

# TODO: there are some other types that aren't as common:
#   intervals, JSON, geography
_PY_TYPE_TO_BQ_TYPE = {
    int: "INTEGER",
    str: "STRING",
    float: "FLOAT",
    datetime.datetime: "TIMESTAMP",
    bytes: "BYTES",
    bool: "BOOL",
    datetime.date: "DATE",
    datetime.time: "TIME",
}


def dataclass_to_json_bq_schema(type_: Type) -> str:
    """Convert a dataclass to a list of bigquery.SchemaField objects.

    Args:
        type_: The dataclass type to convert.

    Returns:
        A list of bigquery.SchemaField objects.
    """
    schema_fields = dataclass_fields_to_bq_schema(dataclasses.fields(type_))
    json_fields = [f.to_api_repr() for f in schema_fields]
    return json.dumps(json_fields)


def dataclass_fields_to_bq_schema(
    fields: dataclasses.Field,
) -> List[bigquery.SchemaField]:
    bq_fields = []
    for field in fields:
        name = field.name
        field_type = field.type
        mode = "REQUIRED"
        if (
            hasattr(field.type, "__args__")
            and len(field.type.__args__) == 2
            and field.type.__args__[-1] is type(None)
        ):  # noqa: E721
            mode = "NULLABLE"
            field_type = field.type.__args__[0]
        if hasattr(field.type, "__args__") and field.type.__origin__ is list:
            mode = "REPEATED"
            field_type = field.type.__args__[0]
        if dataclasses.is_dataclass(field_type):
            sub_fields = dataclass_fields_to_bq_schema(dataclasses.fields(field_type))
            bq_fields.append(
                bigquery.SchemaField(
                    name=name, field_type="RECORD", mode=mode, fields=sub_fields
                )
            )
        elif field_type in _PY_TYPE_TO_BQ_TYPE:
            bq_fields.append(
                bigquery.SchemaField(
                    name=name, field_type=_PY_TYPE_TO_BQ_TYPE[field_type], mode=mode
                )
            )
        else:
            raise ValueError(f"Can't convert type: {field_type} to a bigquery schema")
    return bq_fields
