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


# TODO: Find a better fix for this.
# Context: dataclasses.fields(type) is not working inside a remote ray Actor / task when
# the type arg is fetched using inspect.getfullargspec(...).annotations['return']
def _dataclass_fields(class_or_instance):
    """Return a tuple describing the fields of this dataclass.

    Accepts a dataclass or an instance of one. Tuple elements are of
    type Field.
    """

    # Might it be worth caching this, per class?
    try:
        fields = getattr(class_or_instance, dataclasses._FIELDS)
    except AttributeError:
        raise TypeError("must be called with a dataclass type or instance")

    # Exclude pseudo-fields.  Note that fields is sorted by insertion
    # order, so the order of the tuple is as the fields were defined.
    return tuple(
        f for f in fields.values() if f._field_type.__class__.__name__ == "_FIELD_BASE"
    )


def dataclass_to_json_bq_schema(type_: Type) -> str:
    """Convert a dataclass to a list of bigquery.SchemaField objects.

    Args:
        type_: The dataclass type to convert.

    Returns:
        A list of bigquery.SchemaField objects.
    """
    schema_fields = dataclass_fields_to_bq_schema(_dataclass_fields(type_))
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
            sub_fields = dataclass_fields_to_bq_schema(_dataclass_fields(field_type))
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
