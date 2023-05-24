import asyncio
import signal

from buildflow.api import grid

async def shutdown(results):
    print('Shutting down grid...')
    shutdowns = []
    for result in results:
        shutdowns.append(result.shutdown())
    await asyncio.gather(*shutdowns)
    print('...grid shut down.')


class DeploymentGrid(grid.GridAPI):

    def deploy(self):
        asyncio.run(self._deploy_async())

    async def _deploy_async(self):
        results = []
        for node in self.nodes.values():
            results.append(node.node.run(blocking=False))
            
        coros = [result.output(register_shutdown=False) for result in results]
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig,
                                    lambda: asyncio.create_task(shutdown(results)))
        return await asyncio.wait(coros)