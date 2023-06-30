import enum
from buildflow.api_v2.node.infra.infra_options import InfraOptions
from buildflow.api_v2.node.infra.infra_state import InfraState


class InfraStatus(enum.Enum):
    IDLE = enum.auto()
    PLANNING = enum.auto()
    APPLYING = enum.auto()
    DESTROYING = enum.auto()


class InfraAPI:
    options: InfraOptions
    state: InfraState

    async def plan(self):
        """Returns a plan for the infrastructure."""
        raise NotImplementedError("plan not implemented")

    async def apply(self):
        """Applies the plan to the infrastructure."""
        raise NotImplementedError("apply not implemented")

    async def destroy(self):
        """Destroys the infrastructure."""
        raise NotImplementedError("destroy not implemented")

    async def status(self) -> InfraStatus:
        """Returns the current status of the infra runner."""
        raise NotImplementedError("status not implemented")
