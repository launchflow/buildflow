# ruff: noqa: E501
from buildflow.api.workspaces.workspace.projects.project.deployments.deployment.node._shared.patterns._shared.endpoint._endpoint import (
    EndpointAPI,
)
from buildflow.api.workspaces.workspace.projects.project.deployments.deployment.node._shared.patterns._shared.endpoint.resource.provider import (
    Request,
)
from buildflow.api.workspaces.workspace.projects.project.deployments.deployment.node._shared.patterns._shared.sink._sink import (
    SinkAPI,
)
from buildflow.api.workspaces.workspace.projects.project.deployments.deployment.node._shared.patterns._shared.sink.resource.provider import (
    Batch,
)

CollectorID = str


class CollectorAPI:
    collector_id: CollectorID
    endpoint: EndpointAPI
    sink: SinkAPI

    # This lifecycle method is called once per replica.
    def setup(self):
        raise NotImplementedError("setup not implemented")

    # This lifecycle method is called once per payload.
    def process(self, request: Request, **kwargs) -> Batch:
        raise NotImplementedError("process not implemented")
