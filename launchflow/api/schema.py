from typing import Any, Dict, Mapping, Optional, Sequence, TypeVar, Union

from google.cloud import bigquery

from launchflow.api import resources

# This is the type annotation stored in bigquery.Table(...) objects.
BIGQUERY_SCHEMA = TypeVar('BIGQUERY_SCHEMA',
                          bound=Optional[Sequence[Union[bigquery.SchemaField,
                                                        Mapping[str, Any]]]])


class Schema:
    """Super class for all schema types."""
    _resource_type: resources.IO
    _fields: Dict[str, Any]

    @classmethod
    def from_bigquery_schema(cls, bq_schema: BIGQUERY_SCHEMA):
        raise NotImplementedError('from_bigquery_schema not implemented')

    def to_bigquery_schema(self) -> BIGQUERY_SCHEMA:
        raise NotImplementedError('to_bigquery_schema not implemented')

    def __eq__(self, other: 'Schema') -> bool:
        return self._resource_type == other._resource_type and \
            self._fields == other._fields

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self._resource_type}, {self._fields})'  # noqa

    def __str__(self) -> str:
        return self.__repr__()

    def __hash__(self) -> int:
        return hash((self._resource_type, self._fields))

    def __getitem__(self, key: str) -> Any:
        return self._fields[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self._fields[key] = value

    def __copy__(self) -> 'Schema':
        return self.__class__(self._resource_type, self._fields)

    def __deepcopy__(self, memo: Dict[str, Any]) -> 'Schema':
        return self.__class__(self._resource_type, self._fields)

    def __iter__(self) -> Any:
        return iter(self._fields)
