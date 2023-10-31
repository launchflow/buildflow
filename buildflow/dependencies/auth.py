import logging
from dataclasses import dataclass
from typing import Optional, Type

from google.auth.transport import requests
from google.oauth2 import id_token
from googleapiclient.errors import HttpError
from starlette.requests import Request
from starlette.websockets import WebSocket

from buildflow.dependencies import Scope, dependency
from buildflow.dependencies.headers import BearerCredentials
from buildflow.exceptions import HTTPException


@dataclass
class GoogleUser:
    google_account_id: str
    email: str
    email_verified: bool = False
    name: Optional[str] = None


def AuthenticatedGoogleUserDepBuilder(
    raise_on_unauthenticated: bool = True,
    session_id_token: Optional[str] = None,
    bearer_credentials_dependency: Type = BearerCredentials,
):
    @dependency(scope=Scope.PROCESS)
    class AuthenticatedGoogleUser:
        def __init__(
            self,
            credentials: bearer_credentials_dependency,
            request: Request = None,
            ws: WebSocket = None,
        ):
            if request is None:
                request = ws
            self.google_user = None
            if (
                session_id_token is not None
                and request.session.get(session_id_token) is not None
            ):
                token = request.session[session_id_token]
            elif credentials.token is not None:
                token = credentials.token
            else:
                if raise_on_unauthenticated:
                    raise HTTPException(403, "request is not authenticated")
                return
            try:
                id_info = id_token.verify_oauth2_token(token, requests.Request())
                if not id_info["email_verified"] and raise_on_unauthenticated:
                    raise HTTPException(403, "email is not verified")
                self.google_user = GoogleUser(
                    google_account_id=id_info["sub"],
                    email=id_info["email"],
                    name=id_info.get("name"),
                    email_verified=id_info["email_verified"],
                )
            except HttpError as e:
                logging.exception("Failed to verify token")
                raise HTTPException(status_code=401, detail=str(e))

    return AuthenticatedGoogleUser
