from buildflow.core.credentials._credentials import Credentials
from buildflow.core.options.credentials_options import CredentialsOptions


class AWSCredentials(Credentials):
    def __init__(self, credentials_options: CredentialsOptions) -> None:
        super().__init__(credentials_options)

        self.access_key_id = credentials_options.aws_credentials_options.access_key_id
        self.secret_access_key = (
            credentials_options.aws_credentials_options.secret_access_key
        )
        self.session_token = credentials_options.aws_credentials_options.session_token
