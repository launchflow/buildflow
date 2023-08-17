class BackgroundTask:
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
