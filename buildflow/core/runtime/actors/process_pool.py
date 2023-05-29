import asyncio
import random
from typing import Dict

import ray
from ray.util.metrics import Gauge

from buildflow import utils
from buildflow.api import RuntimeAPI, RuntimeSnapshot, RuntimeStatus
from buildflow.core.runtime.actors.pull_process_push import PullProcessPushActor  # noqa
from buildflow.core.processor.base import Processor
from buildflow.core.runtime.config import RuntimeConfig
# TODO: add ability to load from env vars so we can set num_cpus
# from buildflow.utils import load_config_from_env


@ray.remote(num_cpus=0.1)
class ProcessorReplicaPoolActor(RuntimeAPI):
    """
    This actor acts as a proxy reference for a group of replica Processors.
    Runtime methods are forwarded to the replicas (i.e. "drain"). Includes
    methods for adding and removing replicas (for autoscaling).
    """

    def __init__(self, processor: Processor, config: RuntimeConfig) -> None:
        # configuration
        self.processor = processor
        self.num_threads = config.num_threads_per_process
        # initial runtime state
        self.replicas: Dict[str, PullProcessPushActor] = {}
        self._status = RuntimeStatus.IDLE
        # metrics
        self.num_replicas_gauge = Gauge(
            "processor_pool_num_replicas",
            description="Current number of replicas. Goes up and down.",
            tag_keys=("processor_name", ),
        )
        self.num_replicas_gauge.set_default_tags(
            {"processor_name": self.processor.name})

    def add_replicas(self, num_replicas: int):
        if self._status != RuntimeStatus.RUNNING:
            raise RuntimeError(
                'Can only replicas to a processor pool that is running.')
        for _ in range(num_replicas):
            replica_id = utils.uuid()
            self.replicas[replica_id] = PullProcessPushActor.remote(
                self.processor)
            if self._status == RuntimeStatus.RUNNING:
                for _ in range(self.num_threads):
                    self.replicas[replica_id].run.remote()

        self.num_replicas_gauge.set(len(self.replicas))

    async def remove_replicas(self, num_replicas: int):
        if len(self.replicas) < num_replicas:
            raise ValueError(
                f'Cannot remove {num_replicas} replicas from '
                f'{self.processor.name}. Only {len(self.replicas)} replicas '
                'exist.')

        actors_to_kill = []
        actor_drain_tasks = []
        # TODO: (maybe) Dont just kill randomly
        replicas_to_remove = random.sample(self.replicas.keys(), num_replicas)
        for replica_id in replicas_to_remove:
            replica = self.replicas.pop(replica_id)
            actors_to_kill.append(replica)
            actor_drain_tasks.append(replica.drain.remote())

        _, pending_tasks = await asyncio.wait(actor_drain_tasks, timeout=15)
        for task in pending_tasks:
            # This can happen if the actor is not started yet, we will just
            # force it to with ray.kill below.
            task.cancel()

        # actor_status_tasks = [
        #     actor.status.remote() for actor in actors_to_kill
        # ]
        # actor_statuses = await asyncio.gather(*actor_status_tasks)
        # for i, status in enumerate(actor_statuses):
        #     if status != RuntimeStatus.IDLE:
        #         # TODO: Add diagnostics / handle the case where the actor did
        #         # not shut down properly. We'll probably need this if we want
        #         # to guarantee certain types of "correctness" for a program.
        #         print(f'Actor {actors_to_kill[i]} did not drain properly.')
        for actor in actors_to_kill:
            ray.kill(actor, no_restart=True)

        self.num_replicas_gauge.set(len(self.replicas))

    def start(self):
        print('Starting Processor Pool...')
        if self._status != RuntimeStatus.IDLE:
            raise RuntimeError('Can only start an Idle Runtime.')
        self._status = RuntimeStatus.RUNNING
        for replica in self.replicas.values():
            replica.run.remote()

    async def drain(self):
        print('Draining Processor Pool...')
        self._status = RuntimeStatus.DRAINING
        await self.remove_replicas(len(self.replicas))
        self._status = RuntimeStatus.IDLE
        print('Drain Processor Pool complete')
        return True

    async def status(self):
        if self._status == RuntimeStatus.DRAINING:
            for replica in self.replicas.values():
                if await replica.status.remote() != RuntimeStatus.IDLE:
                    return RuntimeStatus.DRAINING
            self._status = RuntimeStatus.IDLE
        return self._status

    async def snapshot(self):
        # TODO: Add info about sources and whatnot for autoscaler
        return RuntimeSnapshot(...)
