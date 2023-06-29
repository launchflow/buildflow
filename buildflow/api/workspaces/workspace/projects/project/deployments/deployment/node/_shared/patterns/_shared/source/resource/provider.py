# ruff: noqa: E501
from buildflow.api.workspaces.workspace.projects.project.deployments.deployment.node._shared.patterns._shared._shared.resource.provider import (
    ProviderAPI,
)

from typing import Any, Callable, Type, Iterable


class AckInfo:
    pass


class PullResponse:
    payload: Iterable[Any]
    ack_info: AckInfo


class SourceProvider(ProviderAPI):
    async def pull(self) -> PullResponse:
        """Pull returns a batch of data from the source."""
        raise NotImplementedError("pull not implemented")

    async def ack(self, to_ack: AckInfo, success: bool):
        """Ack acknowledges data pulled from the source."""
        raise NotImplementedError("ack not implemented")

    async def backlog(self) -> int:
        """Backlog returns an integer representing the number of items in the backlog"""
        raise NotImplementedError("backlog not implemented")

    def max_batch_size(self) -> int:
        """max_batch_size returns the max number of items that can be pulled at once."""
        raise NotImplementedError("max_batch_size not implemented")

    def pull_converter(self, user_defined_type: Type) -> Callable[[Any], Any]:
        raise NotImplementedError("pull_converter not implemented")
