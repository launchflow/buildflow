from buildflow.api_v2.node.pattern.primitive._primitives.socket.socket_resource import (
    SocketResource,
)
from buildflow.api_v2.node.pattern.primitive.primitive_api import PrimitiveAPI


class Socket(PrimitiveAPI):
    resource: SocketResource
