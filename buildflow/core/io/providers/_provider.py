import dataclasses
from typing import Any, Dict, Iterable, Optional, Type
from buildflow.api.composites.sink import Sink
from buildflow.api.patterns.processor import Processor, ProcessorID
from buildflow.api.composites.source import Source
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


class Provider:
    # NOTE: You can return anything that inherits from pulumi.Resource
    # (i.e. pulumi.ComponentResource)
    # TODO: Update to to use a BuildFlow.Schema abstraction rather than a python Type
    def pulumi_resources(self, type_: Optional[Type]) -> PulumiResources:
        """Provides a list of pulumi.Resources to setup prior to runtime."""
        raise NotImplementedError("pulumi_resources not implemented")


class ProcessorProvider(Provider):
    processor_id: ProcessorID

    def processor(self) -> Processor:
        """Provides a Processor to run at runtime."""
        raise NotImplementedError("processor not implemented")


class SourceProvider(Provider):
    def source(self) -> Source:
        """Provides a Source to read from at runtime."""
        raise NotImplementedError("source not implemented")


class SinkProvider(Provider):
    def sink(self) -> Sink:
        """Provides a Sink to write to at runtime."""
        raise NotImplementedError("sink not implemented")
