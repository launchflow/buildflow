from typing import TypeVar

from buildflow.core.options.credentials_options import CredentialsOptions


class Credentials:
    """Container class for different types of credentials we support."""

    def __init__(self, credentials_options: CredentialsOptions) -> None:
        pass


CredentialType = TypeVar("CredentialType", bound=Credentials)
