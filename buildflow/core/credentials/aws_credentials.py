from buildflow.core.credentials._credentials import Credentials
from buildflow.core.options.credentials_options import CredentialsOptions


class AWSCredentials(Credentials):
    def __init__(self, credentials_options: CredentialsOptions) -> None:
        super().__init__(credentials_options)
        # TODO: Implement this
