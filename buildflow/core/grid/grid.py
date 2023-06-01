import asyncio
import signal

from buildflow.api import GridAPI


async def drain(results):
    print("Shutting down grid...")
    drain_tasks = []
    for result in results:
        drain_tasks.append(result.drain())
    await asyncio.gather(*drain_tasks)
    print("...grid shut down.")


class DeploymentGrid(GridAPI):
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

        coros = [result.output(register_drain=False) for result in results]
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(drain(results)))
        return await asyncio.wait(coros)
