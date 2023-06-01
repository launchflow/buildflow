import dataclasses
from typing import Dict, Any, Iterable
import pulumi


class Batch:
    @classmethod
    def empty(cls):
        raise NotImplementedError("empty not implemented")

    def __iter__(self):
        raise NotImplementedError("__iter__ not implemented")


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

    async def pull(self) -> Batch:
        """Pull returns a batch of data from the source."""
        raise NotImplementedError("pull not implemented")

    async def ack(self):
        """Ack acknowledges data pulled from the source."""
        raise NotImplementedError("ack not implemented")

    async def backlog(self) -> int:
        """Backlog returns an integer representing the number of items in the backlog"""
        raise NotImplementedError("backlog not implemented")


class PushProvider(ProviderAPI):
    """PushProvider is a provider that can have a batch of data pushed to it.

    The following methods should be implemented:
        - push()
    """

    async def push(self, batch: Batch):
        """Push pushes a batch of data to the source."""
        raise NotImplementedError("push not implemented")


# NOTE: SetupProviders set up resources at RUNTIME, not at BUILD_TIME.
class SetupProvider(ProviderAPI):
    """SetupProvider is a provider that sets up any resources needed.

    The following methods should be implemented:
        - setup()
    """

    async def setup(self):
        """Setup sets up any resources needed."""
        raise NotImplementedError("setup not implemented")


# Should we get rid of this? I could see a variant of this being useful.
class PlanProvider(ProviderAPI):
    """PlanProvider is a provider that produces a plan of the resources it will use.

    The following methods should be implemented:
        - plan()
    """

    async def plan(self) -> Dict[str, Any]:
        """Plan produces a plan of the resources it will use."""
        raise NotImplementedError("plan not implemented")


@dataclasses.dataclass
class PulumiResources:
    resources: Iterable[pulumi.Resource]
    exports: Dict[str, Any]


# NOTE: PulumiProviders set up resources at BUILD_TIME, not at RUNTIME.
class PulumiProvider(ProviderAPI):
    """PulumiProvider is a provider that sets up any resources needed using Pulumi.

    Pulumi Docs: https://www.pulumi.com/docs/

    Some Notes:
        - BuildFlow uses Pulumi "Inline Programs"
        - We do not currently support remote deployments (via pulumi cloud)
            - All deployments use the LocalWorkspace interface (from pulumi.automation)
        - Pulumi lets you "export" values from a resource so they can be viewed in the
          console output.

    The following methods should be implemented:
        - pulumi()
    """

    # NOTE: You can return anything that inherits from pulumi.Resource
    # (i.e. pulumi.ComponentResource)
    def pulumi(self) -> PulumiResources:
        """Provides a list of pulumi.Resources to setup prior to runtime."""
        raise NotImplementedError("pulumi not implemented")
