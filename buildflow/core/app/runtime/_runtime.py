import dataclasses
import enum
from typing import Dict, Optional

RunID = str


class RuntimeStatus(enum.Enum):
    PENDING = enum.auto()
    RUNNING = enum.auto()
    DRAINING = enum.auto()
    DRAINED = enum.auto()
    DIED = enum.auto()
    STOPPED = enum.auto()
    # Used for local reloading
    RELOADING = enum.auto()


@dataclasses.dataclass
class RuntimeStatusReport:
    status: RuntimeStatus
    processor_group_statuses: Dict[str, RuntimeStatus]

    def to_dict(self):
        return {
            "status": self.status.name,
            "processor_group_statuses": {
                k: v.name for k, v in self.processor_group_statuses.items()
            },
        }


@dataclasses.dataclass
class RuntimeEvent:
    run_id: RunID
    status_change: Optional[RuntimeStatusReport]

    def to_dict(self):
        return {
            "run_id": self.run_id,
            "status_change": self.status_change.to_dict(),
        }


class Snapshot:
    status: RuntimeStatus
    timestamp_millis: int

    def as_dict(self) -> dict:
        """Returns a dictionary representation of the snapshot"""
        raise NotImplementedError("as_dict not implemented")


class Runtime:
    run_id: RunID

    async def run(self, run_id: RunID) -> bool:
        """Starts the runtime."""
        raise NotImplementedError("run not implemented")

    async def drain(self) -> bool:
        """Sends the drain signal to the runtime."""
        raise NotImplementedError("drain not implemented")

    async def status(self) -> RuntimeStatus:
        """Returns the current status of the runtime."""
        raise NotImplementedError("status not implemented")

    async def snapshot(self) -> Snapshot:
        """Returns a snapshot of the runtime's state."""
        raise NotImplementedError("snapshot not implemented")
