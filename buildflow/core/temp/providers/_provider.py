from buildflow.core.strategies.source import SourceStrategy
from buildflow.core.strategies.sink import SinkStrategy
from buildflow.core.resources.pulumi import PulumiResource
from typing import List


ProviderID = str


class ProviderAPI:
    provider_id: ProviderID


class PulumiProvider(ProviderAPI):
    def pulumi_resources(self) -> List[PulumiResource]:
        raise NotImplementedError("pulumi_resources not implemented for Provider")


class SourceProvider(ProviderAPI):
    def source(self) -> SourceStrategy:
        raise NotImplementedError("source not implemented for Provider")


class SinkProvider(ProviderAPI):
    def sink(self) -> SinkStrategy:
        raise NotImplementedError("sink not implemented for Provider")
