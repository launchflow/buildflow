import dataclasses
from typing import Optional, Type

import psycopg2

from buildflow.core.credentials.empty_credentials import EmptyCredentials
from buildflow.io.provider import ClientProvider
from sqlalchemy.orm import Session


class PostgresProvider(ClientProvider):
    def __init__(
        self,
        *,
        database_name: str,
        host: str,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self.database_name = database_name
        self.host = host
        self.port = port
        self.user = user
        self.password = password

    def client(self, credentials: EmptyCredentials, client_type: Optional[Type]):
        if client_type == psycopg2.connection or client_type is None:
            return psycopg2.connect(
                database=self.database_name,
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
            )
        elif client_type == Session:
            engine = 
            
        else:
            raise ValueError(f"Unsupported client type {client_type}")
