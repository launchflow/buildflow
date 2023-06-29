# ruff: noqa: E501
from buildflow.api.workspaces.workspace.projects.project.deployments.deployment.node._shared.state import (
    State,
)
from buildflow.api.workspaces.workspace.projects.project.deployments.deployment.node.infra._infra import (
    InfraStatus,
)


class InfraState(State):
    status: InfraStatus
    timestamp_millis: int
