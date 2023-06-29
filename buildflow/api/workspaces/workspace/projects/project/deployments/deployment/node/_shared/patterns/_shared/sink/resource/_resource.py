# ruff: noqa: E501
from buildflow.api.workspaces.workspace.projects.project.deployments.deployment.node._shared.patterns._shared._shared.resource import (
    ResourceAPI,
)

from buildflow.api.workspaces.workspace.projects.project.deployments.deployment.node._shared.patterns._shared.sink.resource.provider import (
    SinkProvider,
)


class SinkResource(ResourceAPI):
    provider: SinkProvider
