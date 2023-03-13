"""Utilities for working with BigQuery schemas."""

import dataclasses
import datetime
from typing import Any, Dict, List
import yaml

from google.cloud import bigquery

# TODO: there are some other types that aren't as common:
#   intervals, JSON, geography
_PY_TYPE_TO_BQ_TYPE = {
    int: 'INTEGER',
    str: 'STRING',
    float: 'FLOAT',
    datetime.datetime: 'TIMESTAMP',
    bytes: 'BYTES',
    bool: 'BOOL',
    datetime.date: 'DATE',
    datetime.time: 'TIME',
}


def dataclass_to_bq_schema(
        fields: dataclasses.Field) -> List[bigquery.SchemaField]:
    bq_fields = []
    for field in fields:
        name = field.name
        field_type = field.type
        mode = 'REQUIRED'
        if (hasattr(field.type, "__args__") and len(field.type.__args__) == 2
                and field.type.__args__[-1] is type(None)):  # noqa: E721
            mode = 'NULLABLE'
            field_type = field.type.__args__[0]
        if (hasattr(field.type, "__args__") and field.type.__origin__ is list):
            mode = 'REPEATED'
            field_type = field.type.__args__[0]
        if dataclasses.is_dataclass(field_type):
            sub_fields = dataclass_to_bq_schema(dataclasses.fields(field_type))
            bq_fields.append(
                bigquery.SchemaField(name=name,
                                     field_type='RECORD',
                                     mode=mode,
                                     fields=sub_fields))
        elif field_type in _PY_TYPE_TO_BQ_TYPE:
            bq_fields.append(
                bigquery.SchemaField(
                    name=name,
                    field_type=_PY_TYPE_TO_BQ_TYPE[field_type],
                    mode=mode))
        else:
            raise ValueError(
                f'Can\'t convert type: {field_type} to a bigquery schema')
    return bq_fields


def _schema_field_to_dict(
        schema_field: bigquery.SchemaField) -> Dict[str, Any]:
    field_dict = {
        'name': schema_field.name,
        'type': schema_field.field_type,
        'mode': schema_field.mode,
    }
    if schema_field.fields:
        field_dict['fields'] = [
            _schema_field_to_dict(f) for f in schema_field.fields
        ]

    return field_dict


def schema_fields_to_str(schema_fields: List[bigquery.SchemaField]) -> str:
    yaml_objs = [
        yaml.dump(_schema_field_to_dict(f), sort_keys=False)
        for f in schema_fields
    ]
    return '\n\n'.join(yaml_objs)
