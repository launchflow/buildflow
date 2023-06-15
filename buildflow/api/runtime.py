from dataclasses import dataclass
import enum


class RuntimeStatus(enum.Enum):
    IDLE = enum.auto()
    RUNNING = enum.auto()
    DRAINING = enum.auto()


@dataclass
class SnapshotSummary:
    status: RuntimeStatus
    timestamp_millis: int

    def as_dict(self) -> dict:
        """Returns a dictionary representation of the snapshot"""
        raise NotImplementedError("as_dict not implemented")


@dataclass
class Snapshot:
    status: RuntimeStatus

    def get_timestamp_millis(self) -> int:
        """Returns the timestamp of the snapshot (as millis since epoch)"""
        raise NotImplementedError("get_timestamp not implemented")

    # TODO: Do we ever need the entire state? Or should we just use summary exclusively?
    def as_dict(self) -> dict:
        """Returns a dictionary representation of the snapshot"""
        raise NotImplementedError("as_dict not implemented")

    def summarize(self) -> SnapshotSummary:
        """Returns a summary of the snapshot (status + metrics)"""
        raise NotImplementedError("summarize not implemented")


class RuntimeAPI:
    async def run(self, block: bool) -> bool:
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


class AsyncRuntimeAPI:
    async def run(self) -> bool:
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
