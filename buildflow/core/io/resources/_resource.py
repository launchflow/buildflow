from buildflow.core.options.resource_options import ResourceOptions

ResourceID = str


class Resource:
    resource_id: ResourceID
    exclude_from_infra: bool

    @classmethod
    def from_options(cls, resource_options: ResourceOptions) -> "Resource":
        raise NotImplementedError
