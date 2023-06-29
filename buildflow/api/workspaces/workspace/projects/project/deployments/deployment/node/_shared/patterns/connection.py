# ruff: noqa: E501
from buildflow.api.workspaces.workspace.projects.project.deployments.deployment.node._shared.patterns._shared.socket._socket import (
    SocketAPI,
)
from buildflow.api.workspaces.workspace.projects.project.deployments.deployment.node._shared.patterns._shared.socket.resource.provider import (
    Stream,
)

ConnectionID = str


class ConnectionAPI:
    connection_id: ConnectionID
    socket: SocketAPI

    # This lifecycle method is called once per replica.
    def setup(self):
        raise NotImplementedError("setup not implemented")

    # This lifecycle method is called once per payload.
    def process(self, stream: Stream, **kwargs):
        raise NotImplementedError("process not implemented")
