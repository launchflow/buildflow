import enum

RunID = str


class RuntimeStatus(enum.Enum):
    IDLE = enum.auto()
    RUNNING = enum.auto()
    DRAINING = enum.auto()


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
