from buildflow.api_v2.node.pattern.primitive.resource.provider import ProviderAPI

ResourceID = str


class ResourceAPI:
    resource_id: ResourceID
    provider: ProviderAPI
