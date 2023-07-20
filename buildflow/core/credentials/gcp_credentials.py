import logging

import google.auth
from google.auth import exceptions
from google.auth import credentials as gcp_credentials

from buildflow.core.credentials._credentials import Credentials
from buildflow.core.options.credentials_options import CredentialsOptions


class GCPCredentials(Credentials):
    def __init__(self, credentials_options: CredentialsOptions) -> None:
        super().__init__(credentials_options)
        self.service_account_info = (
            credentials_options.gcp_credentials_options.service_account_info
        )

    def get_creds(self, quota_project_id: str = None):
        if self.service_account_info is not None:
            return gcp_credentials.Credentials.from_service_account_info(
                self.service_account_info
            )
        else:
            try:
                creds, _ = google.auth.default(quota_project_id=quota_project_id)
                return creds
            except exceptions.DefaultCredentialsError:
                # if we failed to fetch the credentials fall back to anonymous
                # credentials. This shouldn't normally happen, but can happen if
                # user is running on a machine with now default creds.
                logging.warning(
                    "no default credentials found, using anonymous credentials"
                )
                return google.auth.credentials.AnonymousCredentials()
