# ruff: noqa: E501
from buildflow.api_v2.node.pattern import PatternAPI
from buildflow.api_v2.node.pattern.primitive._primitives.socket import Socket, Stream

ConnectionID = str


class Connection(PatternAPI):
    connection_id: ConnectionID
    socket: Socket

    # This lifecycle method is called once per payload.
    def process(self, stream: Stream, **kwargs):
        raise NotImplementedError("process not implemented")
