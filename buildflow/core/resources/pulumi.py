import dataclasses
from typing import Any, Dict

import pulumi

from buildflow.core.resources._resource import (Resource, ResourceID,
                                                ResourceType)


# NOTE: We currently dont do anything with this type, but we plan on using this
# in the State API to track the Pulumi resources that are created.
@dataclasses.dataclass
class PulumiResource(Resource):
    resource_id: ResourceID
    resource: pulumi.Resource
    exports: Dict[str, Any]
    # hidden from the end user
    resource_type: ResourceType = dataclasses.field(
        default=ResourceType.PULUMI, init=False
    )
