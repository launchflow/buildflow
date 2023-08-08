from typing import List, Optional, Type, TypeVar

import pulumi

from buildflow.core.background_tasks.background_task import BackgroundTask
from buildflow.core.credentials import CredentialType
from buildflow.io.strategies.sink import SinkStrategy
from buildflow.io.strategies.source import SourceStrategy

ProviderID = str


class ProviderAPI:
    provider_id: ProviderID


class PulumiProvider(ProviderAPI):
    # TODO: Update type_ to use a BuildFlow.Schema type
    def pulumi_resource(
        self,
        type_: Optional[Type],
        credentials: CredentialType,
        opts: pulumi.ResourceOptions,
    ) -> pulumi.ComponentResource:
        raise NotImplementedError("pulumi not implemented for Provider")


PPT = TypeVar("PPT", bound=PulumiProvider)


class SourceProvider(ProviderAPI):
    def source(self, credentials: CredentialType) -> SourceStrategy:
        raise NotImplementedError("source not implemented for Provider")


SOT = TypeVar("SOT", bound=SourceProvider)


class SinkProvider(ProviderAPI):
    def sink(self, credentials: CredentialType) -> SinkStrategy:
        raise NotImplementedError("sink not implemented for Provider")


SIT = TypeVar("SIT", bound=SinkProvider)


class BackgroundTaskProvider(ProviderAPI):
    def background_tasks(self, credentials: CredentialType) -> List[BackgroundTask]:
        raise NotImplementedError("background_task not implemented for Provider")


BTT = TypeVar("BTT", bound=BackgroundTaskProvider)
