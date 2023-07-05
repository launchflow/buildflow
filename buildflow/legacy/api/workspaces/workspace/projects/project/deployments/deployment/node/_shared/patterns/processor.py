# ruff: noqa: E501
from buildflow.api.workspaces.workspace.projects.project.deployments.deployment.node._shared.patterns._shared.sink._sink import (
    SinkAPI,
)
from buildflow.api.workspaces.workspace.projects.project.deployments.deployment.node._shared.patterns._shared.sink.resource.provider import (
    Batch,
)
from buildflow.api.workspaces.workspace.projects.project.deployments.deployment.node._shared.patterns._shared.source._source import (
    SourceAPI,
)
from buildflow.api.workspaces.workspace.projects.project.deployments.deployment.node._shared.patterns._shared.source.resource.provider import (
    PullResponse,
)

ProcessorID = str


class ProcessorAPI:
    processor_id: ProcessorID
    source: SourceAPI
    sink: SinkAPI

    # This lifecycle method is called once per replica.
    def setup(self):
        raise NotImplementedError("setup not implemented")

    # This lifecycle method is called once per payload.
    def process(self, payload: PullResponse, **kwargs) -> Batch:
        raise NotImplementedError("process not implemented")
