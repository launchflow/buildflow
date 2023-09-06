import dataclasses
from typing import Optional

from buildflow.io.postgres.providers.postgres_provider import PostgresProvider
from buildflow.io.primitive import Primitive

# TODO: Support Sessions, created like:
# dialect+driver://username:password@host:port/database_name


@dataclasses.dataclass
class Postgres(
    Primitive[
        # Pulumi provider type
        None,
        # Source provider type
        None,
        # Sink provider type
        None,
        # Background task provider type
        None,
        # Client provider type
        PostgresProvider,
    ]
):
    database_name: str
    host: str
    port: int
    user: Optional[str]
    password: Optional[str]

    def options(
        self,
        # Pulumi management options
        managed: bool = False,
    ) -> "Postgres":
        to_ret = super().options(managed)
        return to_ret

    def client_provider(self) -> PostgresProvider:
        return PostgresProvider(
            database_name=self.database_name,
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
        )
