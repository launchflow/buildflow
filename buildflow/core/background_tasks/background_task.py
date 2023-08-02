import enum


class BackgroundTaskLifeCycle(enum.Enum):
    PROCESSOR = "PROCESSOR"
    REPLICA = "replica"


class BackgroundTask:
    def lifecycle(self) -> BackgroundTaskLifeCycle:
        """Defines the lifecycle of the background task.

        REPLICA: The task will have the same lifecycle as the replica.
        PROCESSOR: The task will have the same lifecycle as the processor.
        """
        return BackgroundTaskLifeCycle.REPLICA

    async def start(self) -> "BackgroundTask":
        """Async method for starting a background task.

        Any async work need to setup the task can be done here.
        """
        return self

    async def shutdown(self):
        """Async method for shutting down a background task.

        Any async work need to shutdown the task can be done here.
        """
        pass
