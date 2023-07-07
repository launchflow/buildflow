import enum
from buildflow.api_v2.node.runtime.runtime_options import RuntimeOptions
from buildflow.api_v2.node.runtime.runtime_state import RuntimeState


class RuntimeStatus(enum.Enum):
    IDLE = enum.auto()
    RUNNING = enum.auto()
    DRAINING = enum.auto()


class RuntimeSnapshot:
    status: RuntimeStatus
    timestamp_millis: int

    def as_dict(self) -> dict:
        """Returns a dictionary representation of the snapshot"""
        raise NotImplementedError("as_dict not implemented")


class RuntimeAPI:
    options: RuntimeOptions
    state: RuntimeState

    async def run(self) -> bool:
        """Starts the runtime."""
        raise NotImplementedError("run not implemented")

    async def drain(self) -> bool:
        """Sends the drain signal to the runtime."""
        raise NotImplementedError("drain not implemented")

    async def status(self) -> RuntimeStatus:
        """Returns the current status of the runtime."""
        raise NotImplementedError("status not implemented")

    async def snapshot(self) -> RuntimeSnapshot:
        """Returns a snapshot of the runtime's state."""
        raise NotImplementedError("snapshot not implemented")
