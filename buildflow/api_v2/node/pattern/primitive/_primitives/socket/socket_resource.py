from buildflow.api_v2.node.pattern.primitive._primitives.socket.socket_provider import (
    SocketProvider,
)
from buildflow.api_v2.node.pattern.primitive.resource.resource_api import ResourceAPI


class SocketResource(ResourceAPI):
    provider: SocketProvider
