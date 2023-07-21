from buildflow.core.credentials._credentials import Credentials
from buildflow.core.options.credentials_options import CredentialsOptions


class EmptyCredentials(Credentials):
    """Empty credentials used by strategies that don't need credentials."""

    def __init__(self, credentials_options: CredentialsOptions) -> None:
        super().__init__(credentials_options)
