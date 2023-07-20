import dataclasses
from typing import Optional

from buildflow.core.options._options import Options


@dataclasses.dataclass
class GCPCredentialsOptions(Options):
    # This is a JSON string containing the service account info.
    # It can either be a service account key, or JSON config for workflow
    # identity federation.
    service_account_info: Optional[str]

    @classmethod
    def default(cls) -> "GCPCredentialsOptions":
        return cls(
            service_account_info=None,
        )


@dataclasses.dataclass
class AWSCredentialsOptions(Options):
    access_key_id: Optional[str]
    secret_access_key: Optional[str]
    session_token: Optional[str]

    @classmethod
    def default(cls) -> "AWSCredentialsOptions":
        return cls(
            access_key_id=None,
            secret_access_key=None,
            session_token=None,
        )


@dataclasses.dataclass
class CredentialsOptions(Options):
    gcp_credentials_options: GCPCredentialsOptions
    aws_credentials_options: AWSCredentialsOptions

    @classmethod
    def default(cls) -> "CredentialsOptions":
        return cls(
            gcp_credentials_options=GCPCredentialsOptions.default(),
            aws_credentials_options=AWSCredentialsOptions.default(),
        )
