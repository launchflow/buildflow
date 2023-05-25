import asyncio
import signal

from buildflow.api import grid


async def shutdown(results):
    print("Shutting down grid...")
    shutdowns = []
    for result in results:
        shutdowns.append(result.shutdown())
    await asyncio.gather(*shutdowns)
    print("...grid shut down.")


class DeploymentGrid(grid.GridAPI):
    def deploy(
        self,
        disable_usage_stats: bool = False,
        disable_resource_creation: bool = False,
    ):
        asyncio.run(self._deploy_async(disable_usage_stats, disable_resource_creation))

    async def _deploy_async(
        self, disable_usage_stats: bool, disable_resource_creation: bool
    ):
        results = []
        for node in self.nodes.values():
            results.append(
                node.node.run(
                    blocking=False,
                    disable_usage_stats=disable_usage_stats,
                    disable_resource_creation=disable_resource_creation,
                )
            )

        coros = [result.output(register_shutdown=False) for result in results]
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(results)))
        return await asyncio.wait(coros)
