# import asyncio
# from concurrent.futures import Future
import asyncio
import dataclasses
from typing import Iterable, List

import ray

from buildflow import utils
from buildflow.api import RuntimeAPI, RuntimeStatus, Snapshot
from buildflow.core.processor.base import Processor
from buildflow.core.runtime.actors.autoscaler import AutoscalerActor
from buildflow.core.runtime.actors.process_pool import (
    ProcessorReplicaPoolActor, ProcessorSnapshot)
from buildflow.core.runtime.config import RuntimeConfig


@dataclasses.dataclass
class RuntimeSnapshot(Snapshot):
    # TODO(nit): I dont like this name, but I'm not sure what else to call it.
    processor_snapshots: List[ProcessorSnapshot]

    _timestamp: int = dataclasses.field(default_factory=utils.timestamp_millis)

    def get_timestamp_millis(self) -> int:
        return self._timestamp

    def as_dict(self):
        return dataclasses.asdict(self)


@ray.remote(num_cpus=0.1)
class RuntimeActor(RuntimeAPI):

    def __init__(
        self,
        config: RuntimeConfig,
    ) -> None:
        # configuration
        self.config = config
        # setup
        self._autoscaler = AutoscalerActor.remote(self.config)
        # initial runtime state
        self._status = RuntimeStatus.IDLE
        self._processor_pool_actors = []
        self._autoscale_loop_task = None

    def start(self, *, processors: Iterable[Processor]):
        print('Starting Runtime...')
        if self._status != RuntimeStatus.IDLE:
            raise RuntimeError('Can only start an Idle Runtime.')
        self._status = RuntimeStatus.RUNNING
        self._processor_pool_actors = [
            ProcessorReplicaPoolActor.remote(processor, self.config)
            for processor in processors
        ]
        for actor in self._processor_pool_actors:
            actor.start.remote()
            actor.add_replicas.remote(self.config.num_replicas())

        self._autoscale_loop_task = self._autoscale_loop()

    async def drain(self) -> bool:
        print('Draining Runtime...')
        self._status = RuntimeStatus.DRAINING
        drain_tasks = [
            actor.drain.remote() for actor in self._processor_pool_actors
        ]
        await asyncio.gather(*drain_tasks)
        self._status = RuntimeStatus.IDLE
        print('Drain Runtime complete')
        return True

    async def status(self):
        if self._status == RuntimeStatus.DRAINING:
            for actor in self._processor_pool_actors:
                if await actor.status.remote() != RuntimeStatus.IDLE:
                    return RuntimeStatus.DRAINING
            self._status = RuntimeStatus.IDLE
        return self._status

    async def snapshot(self):
        snapshot_tasks = [
            actor.snapshot.remote() for actor in self._processor_pool_actors
        ]
        processor_snapshots = await asyncio.gather(*snapshot_tasks)
        return RuntimeSnapshot(processor_snapshots=processor_snapshots)

    async def run_until_complete(self):
        if self._autoscale_loop_task is None:
            while self._status != RuntimeStatus.IDLE:
                print('main thread sleeping for 10 seconds')
                await asyncio.sleep(10)
        else:
            await self._autoscale_loop_task

    def is_active(self):
        return self._status != RuntimeStatus.IDLE

    async def _autoscale_loop(self):
        while self._status == RuntimeStatus.RUNNING:

            for processor_pool in self._processor_pool_actors:
                if self._status != RuntimeStatus.RUNNING:
                    break

                processor_snapshot = await processor_pool.snapshot.remote()
                current_num_replicas = len(processor_snapshot.replicas)
                # TODO: This is a valid case we need to handle, but this is
                # also happening during initial setup
                if current_num_replicas == 0:
                    continue
                target_num_replicas = await (
                    self._autoscaler.get_recommended_num_replicas.remote(
                        processor_snapshot))

                num_replicas_delta = target_num_replicas - current_num_replicas
                if num_replicas_delta > 0:
                    processor_pool.add_replicas.remote(num_replicas_delta)
                elif num_replicas_delta < 0:
                    processor_pool.remove_replicas.remote(
                        abs(num_replicas_delta))

            # TODO: Add more control / ocnfiguration around the autoscaler loop
            await asyncio.sleep(5)
