from typing import Callable, Dict, Optional, Protocol

from buildflow.api.providers import Provider


# NOTE: This class is a protocol for dataclasses to catch static typing errors.
class DataclassProtocol(Protocol):
    __dataclass_fields__: Dict
    __dataclass_params__: Dict
    __post_init__: Optional[Callable]


# NOTE: This class represents a single table.
# NOTE: Any methods added to this class will have a client injected into them.
class StorageAPI:

    # This static method defines the storage provider.
    @staticmethod
    def _provider() -> Provider:
        raise NotImplementedError('_provider not implemented')

    # This static method defines the schema for the table.
    @staticmethod
    def _schema() -> DataclassProtocol:
        raise NotImplementedError('_schema not implemented')

    # This static method defines the name for the table.
    @staticmethod
    def _table_name() -> str:
        raise NotImplementedError('_table_name not implemented')
