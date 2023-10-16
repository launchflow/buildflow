from buildflow.core.credentials._credentials import Credentials
from buildflow.core.options.credentials_options import CredentialsOptions


class ClickhouseCredentials(Credentials):
    def __init__(self, credentials_options: CredentialsOptions) -> None:
        super().__init__(credentials_options)

        self.host = credentials_options.clickhouse_credentials_options.host
        self.username = credentials_options.clickhouse_credentials_options.username
        self.password = credentials_options.clickhouse_credentials_options.password
