from typing import List, Optional, Type

from buildflow.core.background_tasks.background_task import BackgroundTask
from buildflow.core.credentials import CredentialType
from buildflow.core.resources.pulumi import PulumiComponentResource
from buildflow.core.strategies.sink import SinkStrategy
from buildflow.core.strategies.source import SourceStrategy

ProviderID = str


class ProviderAPI:
    provider_id: ProviderID


class PulumiProvider(ProviderAPI):
    # TODO: Update type_ to use a BuildFlow.Schema type
    def pulumi(
        self,
        type_: Optional[Type],
        credentials: CredentialType,
    ) -> Optional[PulumiComponentResource]:
        raise NotImplementedError("pulumi_resources not implemented for Provider")


class EmptyPulumiProvider(PulumiProvider):
    def pulumi(
        self,
        type_: Optional[Type],
        credentials: CredentialType,
    ) -> Optional[PulumiComponentResource]:
        return None


class SourceProvider(ProviderAPI):
    def source(self, credentials: CredentialType) -> SourceStrategy:
        raise NotImplementedError("source not implemented for Provider")


class SinkProvider(ProviderAPI):
    def sink(self, credentials: CredentialType) -> SinkStrategy:
        raise NotImplementedError("sink not implemented for Provider")


class BackgroundTaskProvider(ProviderAPI):
    def background_tasks(self, credentials: CredentialType) -> List[BackgroundTask]:
        raise NotImplementedError("background_task not implemented for Provider")
