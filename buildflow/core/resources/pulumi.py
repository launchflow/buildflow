import dataclasses
from typing import Any, Dict, Optional

import pulumi

from buildflow.core.resources._resource import Resource, ResourceID, ResourceType


@dataclasses.dataclass
class PulumiResource(Resource):
    resource_id: ResourceID
    resource: pulumi.Resource
    exports: Dict[str, Any]
    resource_url: Optional[str] = None
    # hidden from the end user
    resource_type: ResourceType = dataclasses.field(
        default=ResourceType.PULUMI, init=False
    )
