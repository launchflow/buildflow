import dataclasses
from enum import Enum


class Method(Enum):
    GET = "GET"
    POST = "POST"
    WEBSOCKET = "WEBSOCKET"


# Route for an endpoint (e.g. /, /diff, /index)
Route = str


@dataclasses.dataclass
class RouteInfo:
    route: Route
    method: Method

    def __post_init__(self):
        if isinstance(self.method, str):
            self.method = Method(self.method.upper())
