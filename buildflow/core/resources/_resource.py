import enum
from typing import Optional


class ResourceType(enum.Enum):
    PULUMI = "pulumi"


ResourceID = str


class Resource:
    resource_id: ResourceID
    resource_type: ResourceType
    resource_url: Optional[str]
