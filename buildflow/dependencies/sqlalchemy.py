from typing import Optional

from google.auth.credentials import Credentials
from google.cloud.sql.connector import Connector, IPTypes
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from buildflow.dependencies import Scope, dependency
from buildflow.dependencies.flow_dependencies import FlowCredentials
from buildflow.io.gcp.cloud_sql_database import CloudSQLDatabase


def engine(
    db: CloudSQLDatabase,
    db_user: str,
    db_password: str,
    credentials: Optional[Credentials] = None,
):
    def getconn() -> Connector:
        with Connector(credentials=credentials) as connector:
            conn = connector.connect(
                instance_connection_string=f"{db.instance.project_id}:{db.instance.region}:{db.instance.instance_name}",
                driver="pg8000",
                user=db_user,
                password=db_password,
                db=db.database_name,
                ip_type=IPTypes.PUBLIC,
            )
        return conn

    return create_engine("postgresql+pg8000://", creator=getconn)


def SessionDep(db_primitive: CloudSQLDatabase, db_user: str, db_password: str):
    DBDependency = db_primitive.dependency()

    @dependency(scope=Scope.REPLICA)
    class _SessionMakerDeb:
        def __init__(self, db: DBDependency, flow_credentials: FlowCredentials) -> None:
            creds = flow_credentials.gcp_credentials.get_creds()
            self.SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=engine(db, db_user, db_password, creds),
            )

    @dependency(scope=Scope.PROCESS)
    class _SessionDeb:
        def __init__(self, sm_dep: _SessionMakerDeb) -> None:
            self.session = sm_dep.SessionLocal()

    return _SessionDeb
