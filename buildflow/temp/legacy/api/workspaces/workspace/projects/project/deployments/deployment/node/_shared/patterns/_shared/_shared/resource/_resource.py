from buildflow.api.workspaces.projects.deployments.node.shared.patterns.shared.shared.resource.provider import (
    ProviderAPI,
)

ResourceID = str


class ResourceAPI:
    resource_id: ResourceID
    provider: ProviderAPI
