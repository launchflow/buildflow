# ruff: noqa: E501
from buildflow.api.workspaces.workspace.projects.project.deployments.deployment.node._shared.patterns._shared._shared.resource import (
    ResourceAPI,
)

from buildflow.api.workspaces.workspace.projects.project.deployments.deployment.node._shared.patterns._shared.socket.resource.provider import (
    SocketProvider,
)


class SocketResource(ResourceAPI):
    provider: SocketProvider
