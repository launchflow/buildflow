# ruff: noqa: E501
from buildflow.api.workspaces.workspace.projects.project.deployments.deployment.node import (
    NodeAPI,
)

DeploymentID = str


class DeploymentAPI:
    deployment_id: DeploymentID
    node: NodeAPI
