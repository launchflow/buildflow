import asyncio
import dataclasses
import logging
import random
from typing import Dict, List, Optional

import ray
from ray.util.metrics import Gauge

from buildflow import utils
from buildflow.api import RuntimeAPI, RuntimeStatus, Snapshot
from buildflow.core.processor.base import Processor
from buildflow.core.runtime.actors.pull_process_push import (
    NUM_CPUS, PullProcessPushActor)
from buildflow.core.runtime.config import RuntimeConfig
from buildflow.io.providers.base import PullProvider, PushProvider

# TODO: add ability to load from env vars so we can set num_cpus
# from buildflow.utils import load_config_from_env


@dataclasses.dataclass
class ProviderInfo:
    provider_type: str
    provider_config: dict


@dataclasses.dataclass
class SourceInfo:
    backlog: float
    provider: ProviderInfo


@dataclasses.dataclass
class SinkInfo:
    provider: ProviderInfo


@dataclasses.dataclass
class RayActorInfo:
    num_cpus: float


# TODO: Explore using a UtilizationScore that each replica can update
# and then we can use that to determine how many replicas we need.
@dataclasses.dataclass
class ReplicaSnapshot(Snapshot):
    utilization_score: float
    process_rate: float
    actor_info: RayActorInfo

    _timestamp: int = dataclasses.field(default_factory=utils.timestamp_millis)

    def get_timestamp_millis(self) -> int:
        return self._timestamp

    def as_dict(self) -> dict:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class ProcessorSnapshot(Snapshot):
    processor_name: str
    source: SourceInfo
    sink: SinkInfo
    replicas: List[ReplicaSnapshot]
    # Does it make sense to store timestamps in nested snapshots?
    # I _think_ so, since snapshot() is async and might happend at different
    # times for each processor.
    _timestamp: int = dataclasses.field(default_factory=utils.timestamp_millis)

    def get_timestamp_millis(self) -> int:
        return self._timestamp

    def as_dict(self) -> dict:
        return dataclasses.asdict(self)


@ray.remote(num_cpus=0.1)
class ProcessorReplicaPoolActor(RuntimeAPI):
    '''
    This actor acts as a proxy reference for a group of replica Processors.
    Runtime methods are forwarded to the replicas (i.e. 'drain'). Includes
    methods for adding and removing replicas (for autoscaling).
    '''

    def __init__(self, processor: Processor, config: RuntimeConfig) -> None:
        # NOTE: Ray actors run in their own process, so we need to configure
        # logging per actor / remote task.
        logging.getLogger().setLevel(config.log_level)

        # configuration
        self.config = config
        self.processor = processor
        # initial runtime state
        self.replicas: Dict[str, PullProcessPushActor] = {}
        self._status = RuntimeStatus.IDLE
        self._last_snapshot_timestamp: Optional[int] = None
        # metrics
        self.num_replicas_gauge = Gauge(
            'processor_pool_num_replicas',
            description='Current number of replicas. Goes up and down.',
            tag_keys=('processor_name', ),
        )
        self.num_replicas_gauge.set_default_tags(
            {'processor_name': self.processor.name})

    def add_replicas(self, num_replicas: int):
        if self._status != RuntimeStatus.RUNNING:
            raise RuntimeError(
                'Can only replicas to a processor pool that is running.')
        for _ in range(num_replicas):
            replica_id = utils.uuid()
            self.replicas[replica_id] = PullProcessPushActor.remote(
                self.processor, log_level=self.config.log_level)
            if self._status == RuntimeStatus.RUNNING:
                for _ in range(self.config.num_threads_per_process):
                    self.replicas[replica_id].run.remote()

        self.num_replicas_gauge.set(len(self.replicas))

    async def remove_replicas(self, num_replicas: int):
        if len(self.replicas) < num_replicas:
            raise ValueError(
                f'Cannot remove {num_replicas} replicas from '
                f'{self.processor.name}. Only {len(self.replicas)} replicas '
                'exist.')

        # TODO: (maybe) Dont choose randomly
        replicas_to_remove = random.sample(self.replicas.keys(), num_replicas)

        actors_to_kill = []
        actor_drain_tasks = []
        for replica_id in replicas_to_remove:
            replica = self.replicas.pop(replica_id)
            actors_to_kill.append(replica)
            actor_drain_tasks.append(replica.drain.remote())

        _, pending_tasks = await asyncio.wait(actor_drain_tasks, timeout=60)
        # NOTE: Replicas run subtasks that each have a drain timeout of 30secs.
        # If they aren't all done within 60secs, we should assume they failed
        # to drain and let the user know.
        if pending_tasks:
            logging.warning(
                f'Killing {len(pending_tasks)} replicas that failed to drain.')
            for task in pending_tasks:
                task.cancel()

        for actor in actors_to_kill:
            ray.kill(actor, no_restart=True)

        self.num_replicas_gauge.set(len(self.replicas))

    def start(self):
        logging.info(f'Starting ProcessorPool({self.processor.name})...')
        if self._status != RuntimeStatus.IDLE:
            raise RuntimeError('Can only start an Idle Runtime.')
        self._status = RuntimeStatus.RUNNING
        for replica in self.replicas.values():
            replica.run.remote()

    async def drain(self):
        logging.info(f'Draining ProcessorPool({self.processor.name})...')
        self._status = RuntimeStatus.DRAINING
        await self.remove_replicas(len(self.replicas))
        self._status = RuntimeStatus.IDLE
        logging.info(f'Drain ProcessorPool({self.processor.name}) complete.')
        return True

    async def status(self):
        if self._status == RuntimeStatus.DRAINING:
            for replica in self.replicas.values():
                if await replica.status.remote() != RuntimeStatus.IDLE:
                    return RuntimeStatus.DRAINING
            self._status = RuntimeStatus.IDLE
        return self._status

    async def snapshot(self) -> ProcessorSnapshot:
        source_provider: PullProvider = self.processor.source().provider()
        source_backlog = await source_provider.backlog()
        source_info = SourceInfo(
            backlog=source_backlog,
            provider=ProviderInfo(provider_type=type(source_provider),
                                  provider_config={}),
        )
        sink_provider: PushProvider = self.processor.sink().provider()
        sink_info = SinkInfo(provider=ProviderInfo(
            provider_type=type(sink_provider), provider_config={}))

        replica_info_list = []
        for replica in list(self.replicas.values()):
            checkin_result = await replica.checkin.remote()
            # TODO: Come up with a way to get this that also lets it be
            # configurable.
            actor_num_cpus = NUM_CPUS
            replica_info = ReplicaSnapshot(
                utilization_score=checkin_result.utilization_score,
                process_rate=checkin_result.process_rate,
                actor_info=RayActorInfo(num_cpus=actor_num_cpus),
            )
            replica_info_list.append(replica_info)

        return ProcessorSnapshot(
            processor_name=self.processor.name,
            source=source_info,
            sink=sink_info,
            replicas=replica_info_list,
        )
