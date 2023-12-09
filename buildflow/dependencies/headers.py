import dataclasses
import inspect
from typing import Any, Dict, Iterable, Optional, Set, Tuple, Type

from starlette.requests import Request
from starlette.websockets import WebSocket

from buildflow.dependencies import Scope, dependency
from buildflow.dependencies.base import Dependency, DependencyWrapper


def parse_credentials(
    authorization_header_value: Optional[str],
) -> Tuple[str, str]:
    if not authorization_header_value:
        return None
    _, _, token = authorization_header_value.partition(" ")
    return token


class SecurityHeader:
    @classmethod
    def service_auth_info(self) -> Dict[str, str]:
        raise NotImplementedError

    @classmethod
    def endpoint_auth_info(self) -> Dict[str, str]:
        raise NotImplementedError


@dependency(scope=Scope.PROCESS)
class BearerCredentials(SecurityHeader):
    def __init__(self, request: Request = None, websocket: WebSocket = None):
        if request is None and websocket is None:
            raise ValueError("request or websocket must be provided")
        if request is None:
            request = websocket
        auth_header = request.headers.get("Authorization")
        self.token: Optional[str] = parse_credentials(auth_header)

    @classmethod
    def service_auth_info(self) -> Dict[str, str]:
        return {
            "Auth": {
                "type": "http",
                "scheme": "bearer",
                "bearerFormat": "Bearer",
            }
        }

    @classmethod
    def endpoint_auth_info(self) -> Dict[str, str]:
        return {"Auth": []}


@dataclasses.dataclass
class OpenAPISecurityWrapper:
    service_auth_info: Dict[str, Any]
    endpoint_auth_info: Dict[str, Any]


def security_dependencies(
    dep_wrappers: Iterable[DependencyWrapper], visited_types: Set[Type] = set()
) -> Iterable[OpenAPISecurityWrapper]:
    security_wrappers = []
    for dep_wrapper in dep_wrappers:
        if (
            inspect.isclass(dep_wrapper.dependency.dependency_fn)
            and issubclass(dep_wrapper.dependency.dependency_fn, SecurityHeader)
            and type(dep_wrapper.dependency.dependency_fn) not in visited_types
        ):
            visited_types.add(dep_wrapper.dependency.dependency_fn)
            security_wrappers.append(
                OpenAPISecurityWrapper(
                    dep_wrapper.dependency.service_auth_info(),
                    dep_wrapper.dependency.endpoint_auth_info(),
                )
            )
        input_deps = []
        full_arg_spec = inspect.getfullargspec(dep_wrapper.dependency.dependency_fn)
        for arg in full_arg_spec.args:
            if arg in full_arg_spec.annotations:
                if isinstance(full_arg_spec.annotations[arg], Dependency):
                    input_deps.append(
                        DependencyWrapper(arg, full_arg_spec.annotations[arg])
                    )
        security_wrappers.extend(security_dependencies(input_deps, visited_types))
    return security_wrappers
