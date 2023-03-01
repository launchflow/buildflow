import dataclasses
from enum import Enum
from typing import Any, Dict


class Provider:
    """Super class for all input and output types."""
    _provider_type: str

    @classmethod
    def from_config(cls, node_info: Dict[str, Any]):
        provider_type = node_info['_provider_type']
        return _PROVIDER_MAPPING[provider_type](**node_info)

    def get_db_url(self):
        raise NotImplementedError('get_db_url not implemented')


class ProviderType(Enum):
    Postgres = 'POSTGRES'


@dataclasses.dataclass
class Postgres(Provider):
    user: str
    password: str
    host: str
    database: str

    def get_db_url(self):
        return f'postgresql://{self.user}:{self.password}@{self.host}/{self.database}'  # noqa


_PROVIDER_MAPPING = {
    ProviderType.Postgres.value: Postgres,
}
