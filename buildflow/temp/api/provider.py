import dataclasses
from typing import Any, Dict, Iterable, Optional, Type
import pulumi


@dataclasses.dataclass
class PulumiResources:
    resources: Iterable[pulumi.Resource]
    exports: Dict[str, Any]

    @classmethod
    def merge(cls, *args: "PulumiResources") -> "PulumiResources":
        resources = []
        exports = {}
        for arg in args:
            resources.extend(arg.resources)
            exports.update(arg.exports)
        return cls(resources=resources, exports=exports)


class ProviderAPI:
    def pulumi_resources(self, type_: Optional[Type]) -> PulumiResources:
        """Provides a list of pulumi.Resources to setup prior to runtime."""
        raise NotImplementedError("pulumi_resources not implemented")
