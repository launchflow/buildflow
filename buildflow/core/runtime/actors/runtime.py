# import asyncio
# from concurrent.futures import Future
from typing import Iterable

import ray
import asyncio

from buildflow.api import RuntimeAPI, RuntimeSnapshot, RuntimeStatus
from buildflow.core.processor.base import Processor
from buildflow.core.runtime.actors.autoscaler import AutoscalerActor
from buildflow.core.runtime.actors.process_pool import ProcessorReplicaPoolActor  # noqa
from buildflow.core.runtime.config import RuntimeConfig

# Simulating a 4 core machine
# (cluster was started with ray start --head --num-cpus=4)
NUM_CORES = 4


@ray.remote(num_cpus=0.1)
class RuntimeActor(RuntimeAPI):

    def __init__(
        self,
        config: RuntimeConfig = RuntimeConfig.IO_BOUND(NUM_CORES - 1),
    ) -> None:
        # configuration
        self.config = config
        # setup
        self._autoscaler = AutoscalerActor.remote(self.config)
        # initial runtime state
        self._status = RuntimeStatus.IDLE
        self._processor_actors = []

    def start(self, *, processors: Iterable[Processor]):
        print('Starting Runtime...')
        if self._status != RuntimeStatus.IDLE:
            raise RuntimeError('Can only start an Idle Runtime.')
        self._status = RuntimeStatus.RUNNING
        self._processor_actors = [
            ProcessorReplicaPoolActor.remote(processor, self.config)
            for processor in processors
        ]
        for actor in self._processor_actors:
            actor.start.remote()
            actor.add_replicas.remote(self.config.num_replicas())

    async def drain(self) -> bool:
        print('Draining Runtime...')
        self._status = RuntimeStatus.DRAINING
        drain_tasks = [
            actor.drain.remote() for actor in self._processor_actors
        ]
        await asyncio.gather(*drain_tasks)
        self._status = RuntimeStatus.IDLE
        print('Drain Runtime complete')
        return True

    async def status(self):
        if self._status == RuntimeStatus.DRAINING:
            for actor in self._processor_actors:
                if await actor.status.remote() != RuntimeStatus.IDLE:
                    return RuntimeStatus.DRAINING
            self._status = RuntimeStatus.IDLE
        return self._status

    async def snapshot(self):
        return RuntimeSnapshot()

    async def run_until_complete(self):
        print('Blocking main thread on Runtime...')
        while self._status != RuntimeStatus.IDLE:
            print('main thread sleeping for 10 seconds')
            await asyncio.sleep(10)
        print('main thread unblocked')

    def is_active(self):
        return self._status != RuntimeStatus.IDLE
