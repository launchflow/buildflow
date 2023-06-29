# ruff: noqa: E501
from buildflow.api.workspaces.workspace.projects.project.deployments.deployment.node._shared.state import (
    State,
)
from buildflow.api.workspaces.workspace.projects.project.deployments.deployment.node.runtime._runtime import (
    RuntimeStatus,
)


class RuntimeState(State):
    status: RuntimeStatus
    timestamp_millis: int
