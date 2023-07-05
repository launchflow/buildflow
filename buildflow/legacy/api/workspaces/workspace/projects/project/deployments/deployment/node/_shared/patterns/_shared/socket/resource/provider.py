# ruff: noqa: E501
from buildflow.api.workspaces.workspace.projects.project.deployments.deployment.node._shared.patterns._shared._shared.resource.provider import (
    ProviderAPI,
)


class Stream:
    def on_request(handle_request):
        raise NotImplementedError("on_request not implemented")

    def on_batch(handle_batch):
        raise NotImplementedError("on_batch not implemented")

    def on_close(cleanup):
        raise NotImplementedError("on_close not implemented")

    def on_error(cleanup):
        raise NotImplementedError("on_error not implemented")

    def on_timeout(cleanup):
        raise NotImplementedError("on_timeout not implemented")

    def on_heartbeat(cleanup):
        raise NotImplementedError("on_heartbeat not implemented")

    def on_connect(cleanup):
        raise NotImplementedError("on_connect not implemented")

    def on_disconnect(cleanup):
        raise NotImplementedError("on_disconnect not implemented")

    def on_reconnect(cleanup):
        raise NotImplementedError("on_reconnect not implemented")


class SocketProvider(ProviderAPI):
    async def open(self) -> Stream:
        """Open connects to the socket and returns a Stream."""
        raise NotImplementedError("open not implemented")

    async def close(self, stream: Stream):
        """Close disconnects the socket for the given Stream."""
        raise NotImplementedError("close not implemented")
