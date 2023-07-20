from typing import List, Optional, Type

from buildflow.core.credentials import CredentialType
from buildflow.core.resources.pulumi import PulumiResource
from buildflow.core.strategies.sink import SinkStrategy
from buildflow.core.strategies.source import SourceStrategy

ProviderID = str


class ProviderAPI:
    provider_id: ProviderID


class PulumiProvider(ProviderAPI):
    # TODO: Update type_ to use a BuildFlow.Schema type
    def pulumi_resources(self, type_: Optional[Type]) -> List[PulumiResource]:
        raise NotImplementedError("pulumi_resources not implemented for Provider")


class EmptyPulumiProvider(PulumiProvider):
    def pulumi_resources(self, type_: Optional[Type]) -> List[PulumiResource]:
        return []


class SourceProvider(ProviderAPI):
    def source(self, credentials: CredentialType) -> SourceStrategy:
        raise NotImplementedError("source not implemented for Provider")


class SinkProvider(ProviderAPI):
    def sink(self, credentials: CredentialType) -> SinkStrategy:
        raise NotImplementedError("sink not implemented for Provider")
