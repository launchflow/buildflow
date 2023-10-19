import dataclasses
from typing import Any, Callable, Iterable, Type

from buildflow.core.credentials import CredentialType
from buildflow.io.strategies._strategy import StategyType, Strategy, StrategyID


class AckInfo:
    pass


@dataclasses.dataclass
class PullResponse:
    payload: Iterable[Any]
    ack_info: AckInfo


class SourceStrategy(Strategy):
    strategy_type = StategyType.SOURCE

    def __init__(self, credentials: CredentialType, strategy_id: StrategyID):
        super().__init__(credentials=credentials, strategy_id=strategy_id)

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

    async def teardown(self):
        """Teardown is called when the source is no longer needed.

        This should perform any cleanup that is needed by the source.
        """
        pass
