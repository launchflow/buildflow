import enum


class ResourceType(enum.Enum):
    PULUMI = "pulumi"


ResourceID = str


class Resource:
    resource_id: ResourceID
    resource_type: ResourceType
