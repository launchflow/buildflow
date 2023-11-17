from typing import Optional

import asyncpg
from google.auth.credentials import Credentials
from google.cloud.sql.connector import Connector, IPTypes, create_async_connector
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker

from buildflow.dependencies import Scope, dependency
from buildflow.dependencies.flow_dependencies import FlowCredentials
from buildflow.io.gcp.cloud_sql_database import CloudSQLDatabase


async def async_engine(
    db: CloudSQLDatabase,
    db_user: str,
    db_password: str,
    async_engine: bool = False,
    credentials: Optional[Credentials] = None,
    **kwargs,
):
    """
    Args:
        db: The database instance to connect to
        db_user: The database user to user for authentication
        db_password: The password for the database user
        credentials: The optional google credentials to use to connect to the
            database if using GCP.
        **kwargs: Additional keyword arguments to pass to the sqlalchemy
         `create_engine` function
    """
    connector = await create_async_connector()

    # initialize Connector object for connections to Cloud SQL
    async def getconn() -> asyncpg.Connection:
        conn: asyncpg.Connection = await connector.connect_async(
            instance_connection_string=f"{db.instance.project_id}:{db.instance.region}:{db.instance.instance_name}",
            driver="asyncpg",
            user=db_user,
            password=db_password,
            db=db.database_name,
            ip_type=IPTypes.PUBLIC,
        )
        return conn

    return create_async_engine("postgresql+asyncpg://", async_creator=getconn, **kwargs)


def engine(
    db: CloudSQLDatabase,
    db_user: str,
    db_password: str,
    async_engine: bool = False,
    credentials: Optional[Credentials] = None,
    **kwargs,
):
    """
    Args:
        db: The database instance to connect to
        db_user: The database user to user for authentication
        db_password: The password for the database user
        credentials: The optional google credentials to use to connect to the
            database if using GCP.
        **kwargs: Additional keyword arguments to pass to the sqlalchemy
         `create_engine` function

    """

    def getconn() -> Connector:
        with Connector(credentials=credentials) as connector:
            conn = connector.connect(
                instance_connection_string=f"{db.instance.project_id}:{db.instance.region}:{db.instance.instance_name}",
                driver="pg80000",
                user=db_user,
                password=db_password,
                db=db.database_name,
                ip_type=IPTypes.PUBLIC,
            )
        return conn

    return create_engine("postgresql+pg8000://", async_creator=getconn, **kwargs)


def SessionDep(
    db_primitive: CloudSQLDatabase,
    db_user: str,
    db_password: str,
    use_async: bool = False,
    **kwargs,
):
    """
    Args:
        db: The database primitive to connect to
        db_user: The database user to user for authentication
        db_password: The password for the database user
        **kwargs: Additional keyword arguments to pass to the sqlalchemy
         `create_engine` function

    """
    DBDependency = db_primitive.dependency()

    @dependency(scope=Scope.REPLICA)
    class _SessionMakerDep:
        async def __init__(
            self, db: DBDependency, flow_credentials: FlowCredentials
        ) -> None:
            creds = flow_credentials.gcp_credentials.get_creds()
            if use_async:
                session_engine = await async_engine(
                    db, db_user, db_password, creds, **kwargs
                )
            else:
                session_engine = engine(db, db_user, db_password, creds, **kwargs)
            self.SessionLocal = sessionmaker(
                autocommit=False, autoflush=False, bind=session_engine
            )

    @dependency(scope=Scope.PROCESS)
    class _SessionDep:
        def __init__(self, sm_dep: _SessionMakerDep) -> None:
            self.session = sm_dep.SessionLocal()

    return _SessionDep
