from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, Type


class Batch:
    @classmethod
    def empty(cls):
        raise NotImplementedError("empty not implemented")

    def __iter__(self):
        raise NotImplementedError("__iter__ not implemented")


class AckInfo:
    pass


@dataclass(frozen=True)
class PullResponse:
    payload: Iterable[Any]
    ack_info: AckInfo


class ProviderAPI:
    def __init__(self):
        pass

    def schema(self):
        raise NotImplementedError("schema not implemented")


class PullProvider(ProviderAPI):
    """PullProvider is a provider that can be pulled from.

    The following methods should be implemented:

    - pull()
    - ack()
    - backlog()
    """

    async def pull(self) -> PullResponse:
        """Pull returns a batch of data from the source."""
        raise NotImplementedError("pull not implemented")

    async def ack(self, to_ack: AckInfo):
        """Ack acknowledges data pulled from the source."""
        raise NotImplementedError("ack not implemented")

    async def backlog(self) -> int:
        """Backlog returns an integer representing the number of items in the backlog"""
        raise NotImplementedError("backlog not implemented")

    def pull_converter(self, user_defined_type: Type) -> Callable[[Any], Any]:
        raise NotImplementedError("pull_converter not implemented")


class PushProvider(ProviderAPI):
    """PushProvider is a provider that can have a batch of data pushed to it.

    The following methods should be implemented:

    - push()
    """

    async def push(self, batch: Batch):
        """Push pushes a batch of data to the source."""
        raise NotImplementedError("push not implemented")

    def push_converter(self, user_defined_type: Type) -> Callable[[Any], Any]:
        raise NotImplementedError("push_converter not implemented")


# TODO: Should we have InfraProvider instead that had plan, apply, destroy?
# Thoughts: Its nice to have a setup() option as a scape goat option for users
# who dont want to implement the Infra api for their custom use case.
class SetupProvider(ProviderAPI):
    """SetupProvider is a provider that sets up any reousrces needed.

    The following methods should be implemented:

    - setup()
    """

    async def setup(self):
        """Setup sets up any resources needed."""
        raise NotImplementedError("setup not implemented")


class PlanProvider(ProviderAPI):
    """PlanProvider is a provider that produces a plan of the resources it will use.

    The following methods should be implemented:

    - plan()
    """

    async def plan(self) -> Dict[str, Any]:
        """Plan produces a plan of the resources it will use."""
        raise NotImplementedError("plan not implemented")
