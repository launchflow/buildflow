import enum


class Snapshot:
    def get_timestamp_millis(self) -> int:
        """Returns the timestamp of the snapshot (as millis since epoch)"""
        raise NotImplementedError("get_timestamp not implemented")

    def as_dict(self) -> dict:
        """Returns a dictionary representation of the snapshot"""
        raise NotImplementedError("as_dict not implemented")


class RuntimeStatus(enum.Enum):
    IDLE = enum.auto()
    RUNNING = enum.auto()
    DRAINING = enum.auto()


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
