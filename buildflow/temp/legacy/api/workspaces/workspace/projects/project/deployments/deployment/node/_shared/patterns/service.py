# ruff: noqa: E501
from buildflow.api.workspaces.workspace.projects.project.deployments.deployment.node._shared.patterns._shared.endpoint._endpoint import (
    EndpointAPI,
)
from buildflow.api.workspaces.workspace.projects.project.deployments.deployment.node._shared.patterns._shared.endpoint.resource.provider import (
    Request,
    Response,
)

ServiceID = str


class ServiceAPI:
    service_id: ServiceID
    endpoint: EndpointAPI

    # This lifecycle method is called once per replica.
    def setup(self):
        raise NotImplementedError("setup not implemented")

    # This lifecycle method is called once per payload.
    def process(self, request: Request, **kwargs) -> Response:
        raise NotImplementedError("process not implemented")
