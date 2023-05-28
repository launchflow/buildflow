import asyncio
import dataclasses
import signal
from concurrent.futures import Future
from typing import Iterable, List

import ray

from buildflow.api import RuntimeAPI, RuntimeSnapshot, RuntimeStatus
from buildflow.core.processor import Processor
from buildflow.core.runtime import AutoscalerActor, ProcessorPoolActor


@dataclasses.dataclass
class Environment:
    pass


@ray.remote
class RuntimeActor(RuntimeAPI):

    def __init__(self, env: Environment) -> None:
        # setup
        self._autoscaler = AutoscalerActor.remote(env)
        # initial runtime state
        self._status = RuntimeStatus.IDLE
        self._tasks: List[Future] = []

    def run(self, *, processors: Iterable[Processor]):
        if self._status != RuntimeStatus.IDLE:
            raise RuntimeError('Can only start an Idle Runtime.')
        self._status = RuntimeStatus.RUNNING
        signal.signal(signal.SIGTERM, self.drain)
        signal.signal(signal.SIGINT, self.drain)
        self._tasks: List[Future] = [
            self._handle_process_pool.remote(
                ProcessorPoolActor(processor)).future()
            for processor in processors
        ]

    def drain(self, block: bool) -> bool:
        self._status = RuntimeStatus.DRAINING
        # NOTE: setting the status to DRAINING will cause the
        # _handle_process_pool method to start draining the processor pools
        if block:
            asyncio.run(asyncio.gather(*self._tasks))
        return True

    def status(self):
        if self._status == RuntimeStatus.DRAINING:
            for task in self._tasks:
                task: Future
                if not task.done():
                    return RuntimeStatus.DRAINING
            self._status = RuntimeStatus.IDLE
        return self._status

    def snapshot(self):
        return RuntimeSnapshot()

    def is_active(self):
        return self._status != RuntimeStatus.IDLE

    def run_until_complete(self) -> bool:
        return self.drain(block=True)

    # NOTE: Each handler runs in its own thread, so we can use blocking calls
    async def _handle_process_pool(self, processor_pool: ProcessorPoolActor):
        while self._status == RuntimeStatus.RUNNING:
            pool_status = await processor_pool.status.remote()
            recommendation = await self._autoscaler.recommendation.remote(
                pool_status)

            processor_pool.add_replicas(recommendation.num_replicas)
        if self._status == RuntimeStatus.DRAINING:
            await processor_pool.drain.remote(block=True)
