# ruff: noqa: E501
from typing import Dict

from buildflow.api.workspaces.workspace.projects.project.deployments.deployment.node._shared.patterns.processor import (
    ProcessorAPI,
    ProcessorID,
)
from buildflow.api.workspaces.workspace.projects.project.deployments.deployment.node.infra import (
    InfraAPI,
)
from buildflow.api.workspaces.workspace.projects.project.deployments.deployment.node.runtime import (
    RuntimeAPI,
)

NodeID = str


class NodeAPI:
    node_id: NodeID
    runtime: RuntimeAPI
    infra: InfraAPI
    processors: Dict[ProcessorID, ProcessorAPI]

    def add(self, processor: ProcessorAPI):
        raise NotImplementedError("add not implemented")

    def plan(self):
        raise NotImplementedError("plan not implemented")

    def run(self):
        raise NotImplementedError("run not implemented")

    def apply(self):
        raise NotImplementedError("apply not implemented")

    def destroy(self):
        raise NotImplementedError("destroy not implemented")
