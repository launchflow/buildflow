from typing import Any, Callable, List, Type

from buildflow.api.composites.sink import Sink
from buildflow.core.io.providers._provider import Provider, PulumiResources


class EmptyProvider(Sink, Provider):
    def pulumi_resources(self, type_: Any | None) -> PulumiResources:
        return PulumiResources(resources=[], exports={})

    def push(self, batch: List[dict]):
        pass

    def push_converter(self, user_defined_type: Type) -> Callable[[Any], Any]:
        pass
