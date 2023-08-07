import dataclasses

import pulumi

from buildflow.core.resources._resource import Resource, ResourceID, ResourceType


@dataclasses.dataclass
class PulumiComponentResource(Resource):
    resource_id: ResourceID
    resource: pulumi.ComponentResource
    # hidden from the end user
    resource_type: ResourceType = dataclasses.field(
        default=ResourceType.PULUMI, init=False
    )
