from typing import Optional, Tuple

from starlette.requests import Request

from buildflow.dependencies import Scope, dependency


def parse_credentials(
    authorization_header_value: Optional[str],
) -> Tuple[str, str]:
    if not authorization_header_value:
        return None
    _, _, token = authorization_header_value.partition(" ")
    return token


@dependency(scope=Scope.PROCESS)
class BearerCredentials:
    def __init__(self, request: Request):
        auth_header = request.headers.get("Authorization")
        self.token: Optional[str] = parse_credentials(auth_header)
