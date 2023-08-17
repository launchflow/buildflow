from typing import Any, Callable, Type

from buildflow.core.credentials import CredentialType
from buildflow.io.strategies._strategy import StategyType, Strategy, StrategyID


class Batch:
    pass


class SinkStrategy(Strategy):
    strategy_type = StategyType.SINK

    def __init__(self, credentials: CredentialType, strategy_id: StrategyID):
        super().__init__(credentials=credentials, strategy_id=strategy_id)

    async def push(self, batch: Batch):
        """Push pushes a batch of data to the source."""
        raise NotImplementedError("push not implemented")

    def push_converter(self, user_defined_type: Type) -> Callable[[Any], Any]:
        raise NotImplementedError("push_converter not implemented")

    async def teardown(self):
        """Teardown is called when the sink is no longer needed.

        This should perform any cleanup that is needed by the sink.
        """
        pass
