import logging
from dataclasses import dataclass
from typing import Optional, Type

from google.auth.transport import requests
from google.oauth2 import id_token
from googleapiclient.errors import HttpError

from buildflow.dependencies import Scope, dependency
from buildflow.dependencies.headers import BearerCredentials
from buildflow.exceptions import HTTPException


@dataclass
class GoogleUser:
    google_account_id: str
    email: str
    name: Optional[str] = None


def AuthenticatedGoogleUserDepBuilder(
    raise_on_unauthenticated: bool = True,
    bearer_credentials_dependency: Type = BearerCredentials,
):
    @dependency(scope=Scope.PROCESS)
    class AuthenticatedGoogleUser:
        def __init__(self, credentials: bearer_credentials_dependency):
            self.google_user = None
            if credentials.token is None:
                if raise_on_unauthenticated:
                    raise HTTPException(403, "request is not authenticated")
                return
            try:
                id_info = id_token.verify_oauth2_token(
                    credentials.token, requests.Request()
                )
                self.google_user = GoogleUser(
                    google_account_id=id_info["sub"],
                    email=id_info["email"],
                    name=id_info.get("name"),
                )
            except HttpError as e:
                logging.exception("Failed to verify token")
                raise HTTPException(status_code=401, detail=str(e))

    return AuthenticatedGoogleUser
