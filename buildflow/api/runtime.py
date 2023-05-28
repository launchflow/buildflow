import enum


class RuntimeSnapshot:

    def as_dict(self) -> dict:
        """Sends the drain signal to the runtime."""
        raise NotImplementedError("drain not implemented")


class RuntimeStatus(enum.Enum):
    IDLE = enum.auto()
    RUNNING = enum.auto()
    DRAINING = enum.auto()


class RuntimeAPI:

    def run(self) -> bool:
        """Starts the runtime."""
        raise NotImplementedError("run not implemented")

    def drain(self, block: bool) -> bool:
        """Sends the drain signal to the runtime."""
        raise NotImplementedError("drain not implemented")

    def status(self) -> RuntimeStatus:
        """Returns the current status of the runtime."""
        raise NotImplementedError("status not implemented")

    def snapshot(self) -> RuntimeSnapshot:
        """Returns a snapshot of the runtime's state."""
        raise NotImplementedError("snapshot not implemented")
