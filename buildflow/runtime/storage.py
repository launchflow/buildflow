import dataclasses
from datetime import datetime
from typing import List, Optional

from buildflow import utils
from buildflow.api import DataclassProtocol, StorageAPI
from buildflow.api.providers import Provider
from buildflow.runtime.runner import Runtime
from sqlalchemy import (ARRAY, TIMESTAMP, Boolean, Column, Float, Integer,
                        MetaData, String, Table, create_engine)
from sqlalchemy.orm import Session, class_mapper, mapper, sessionmaker
from sqlalchemy.orm.exc import UnmappedError


class _ID:
    _type: type = int


def ID(_type: type = int):
    if _type != int:
        raise NotImplementedError('Only int IDs are currently supported.')
    class_name = f'{_type.__name__.title()}ID'
    return type(class_name, (_ID, ), {'_type': _type})


class _PrimaryKey:
    _type: type


def PrimaryKey(_type: type):
    class_name = f'{_type.__name__.title()}PrimaryKey'
    return type(class_name, (_PrimaryKey, ), {'_type': _type})


_type_map = {
    int: Integer,
    float: Float,
    str: String,
    bool: Boolean,
    datetime: TIMESTAMP,
    List[int]: ARRAY(Integer),
    List[float]: ARRAY(Float),
    List[str]: ARRAY(String),
    List[bool]: ARRAY(Boolean),
    List[datetime]: ARRAY(TIMESTAMP),
}


def _parse_dataclass_field_into_column(field: dataclasses.field) -> Column:
    # Handle special types
    if isinstance(field.type, type) and issubclass(field.type, _ID):
        return Column(field.name,
                      _type_map.get(field.type._type),
                      primary_key=True,
                      autoincrement=True)
    if isinstance(field.type, type) and issubclass(field.type, _PrimaryKey):
        return Column(field.name,
                      _type_map.get(field.type._type),
                      primary_key=True)
    # Handle standard types
    orm_type = _type_map.get(field.type, None)
    if orm_type is None:
        raise NotImplementedError(f'No ORM type found for {field.type}')
    return Column(field.name, orm_type)


def _parse_dataclass_into_table(dataclass: DataclassProtocol, table_name: str,
                                metadata: MetaData) -> Table:
    table_args = (
        table_name,
        metadata,
    ) + tuple(
        _parse_dataclass_field_into_column(field)
        for field in dataclasses.fields(dataclass))
    return Table(*table_args)


def _build_orm_session_from_storage_api(storage_api: StorageAPI) -> Session:
    # TODO: Add env var to enable echo=True param
    engine = create_engine(storage_api._provider().get_db_url())

    if storage_api._schema() is not None:
        table_initialzed = True
        try:
            class_mapper(storage_api._schema())
        except UnmappedError:
            table_initialzed = False
        if not table_initialzed:
            metadata = MetaData()
            try:
                table_name = storage_api._table_name()
            except NotImplementedError:
                # Default to using the class name as the table name
                table_name = storage_api._schema().__name__
            table = _parse_dataclass_into_table(storage_api._schema(),
                                                table_name, metadata)
            mapper(storage_api._schema(), table)
            metadata.create_all(engine)

    return sessionmaker(bind=engine)()


def storage(provider: Provider,
            schema: Optional[DataclassProtocol] = None,
            table_name: Optional[str] = None):
    runtime = Runtime()

    def decorator_function(original_function):
        storage_id = original_function.__qualname__
        # Dynamically define a new class with the same structure as StorageAPI
        class_name = f'AdHocStorage_{utils.uuid(max_len=8)}'
        class_methods = {
            '_provider': staticmethod(lambda: provider),
            '_schema': staticmethod(lambda: schema),
        }
        if table_name is not None:
            class_methods['_table_name'] = staticmethod(lambda: table_name)
        _AdHocStorage = type(class_name, (StorageAPI, ), class_methods)
        # Do we need to register the storage? Probably for k8 deployments
        runtime.register_storage(_AdHocStorage, storage_id)

        session = _build_orm_session_from_storage_api(_AdHocStorage)

        def wrapper_function(*args, **kwargs):
            return original_function(*args, **kwargs, session=session)

        return wrapper_function

    return decorator_function
