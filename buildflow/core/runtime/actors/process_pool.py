import asyncio
import dataclasses
import logging
import random
from typing import Dict, List, Optional

import ray
from ray.util.metrics import Gauge
from ray.util.placement_group import placement_group, remove_placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

from buildflow import utils
from buildflow.api import RuntimeAPI, RuntimeStatus, Snapshot, ProcessorID
from buildflow.core.processor.base import Processor
from buildflow.core.runtime.actors.pull_process_push import (
    PullProcessPushActor,
)  # noqa: E501
from buildflow.core.runtime.config import ReplicaConfig
from buildflow.io.providers.base import PullProvider, PushProvider

# TODO: add ability to load from env vars so we can set num_cpus
# from buildflow.utils import load_config_from_env


@dataclasses.dataclass
class ProviderInfo:
    provider_type: str
    provider_config: dict


@dataclasses.dataclass
class SourceInfo:
    backlog: Optional[float]
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

    _timestamp: int = dataclasses.field(default_factory=utils.timestamp_millis)

    def get_timestamp_millis(self) -> int:
        return self._timestamp

    def as_dict(self) -> dict:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class ProcessorSnapshot(Snapshot):
    processor_id: ProcessorID
    source: SourceInfo
    sink: SinkInfo
    replicas: List[ReplicaSnapshot]
    actor_info: RayActorInfo
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
    """
    This actor acts as a proxy reference for a group of replica Processors.
    Runtime methods are forwarded to the replicas (i.e. 'drain'). Includes
    methods for adding and removing replicas (for autoscaling).
    """

    def __init__(self, processor: Processor, config: ReplicaConfig) -> None:
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
        job_id = ray.get_runtime_context().get_job_id()
        self.num_replicas_gauge = Gauge(
            "num_replicas",
            description="Current number of replicas. Goes up and down.",
            tag_keys=("processor_id", "JobId"),
        )
        self.num_replicas_gauge.set_default_tags(
            {
                "processor_id": self.processor.processor_id,
                "JobId": job_id,
            }
        )
        self.concurrency_gauge = Gauge(
            "ppp_concurrency",
            description="Current number of concurrency per replica. Goes up and down.",
            tag_keys=(
                "processor_id",
                "JobId",
            ),
        )
        self.concurrency_gauge.set_default_tags(
            {
                "processor_id": processor.processor_id,
                "JobId": job_id,
            }
        )
        self.concurrency_gauge.set(config.num_concurrent_tasks)

    async def add_replicas(self, num_replicas: int):
        if self._status != RuntimeStatus.RUNNING:
            raise RuntimeError("Can only replicas to a processor pool that is running.")
        for _ in range(num_replicas):
            replica_id = utils.uuid()

            ray_placement_group = await placement_group(
                [
                    {
                        "CPU": self.config.num_cpus,
                    }
                ],
                strategy="STRICT_PACK",
            ).ready()

            replica_actor_handle = PullProcessPushActor.options(
                num_cpus=self.config.num_cpus,
                scheduling_strategy=PlacementGroupSchedulingStrategy(
                    placement_group=ray_placement_group,
                    placement_group_capture_child_tasks=True,
                ),
            ).remote(self.processor, log_level=self.config.log_level)
            if self._status == RuntimeStatus.RUNNING:
                for _ in range(self.config.num_concurrent_tasks):
                    replica_actor_handle.run.remote()

            self.replicas[replica_id] = (replica_actor_handle, ray_placement_group)

        self.num_replicas_gauge.set(len(self.replicas))

    async def remove_replicas(self, num_replicas: int):
        if len(self.replicas) < num_replicas:
            raise ValueError(
                f"Cannot remove {num_replicas} replicas from "
                f"{self.processor.processor_id}. Only {len(self.replicas)} replicas "
                "exist."
            )

        # TODO: (maybe) Dont choose randomly
        replicas_to_remove = random.sample(self.replicas.keys(), num_replicas)

        placement_groups_to_remove = []
        actors_to_kill = []
        actor_drain_tasks = []
        for replica_id in replicas_to_remove:
            replica_actor, ray_placement_group = self.replicas.pop(replica_id)
            actors_to_kill.append(replica_actor)
            actor_drain_tasks.append(replica_actor.drain.remote())
            placement_groups_to_remove.append(ray_placement_group)

        if actor_drain_tasks:
            await asyncio.wait(actor_drain_tasks)

        for actor, pg in zip(actors_to_kill, placement_groups_to_remove):
            ray.kill(actor, no_restart=True)
            # Placement groups are scoped to the ProcessorPool, so we need to
            # manually clean them up.
            remove_placement_group(pg)

        self.num_replicas_gauge.set(len(self.replicas))

    def run(self):
        logging.info(f"Starting ProcessorPool({self.processor.processor_id})...")
        if self._status != RuntimeStatus.IDLE:
            raise RuntimeError("Can only start an Idle Runtime.")
        self._status = RuntimeStatus.RUNNING
        for replica in self.replicas.values():
            replica.run.remote()

    async def drain(self):
        logging.info(f"Draining ProcessorPool({self.processor.processor_id})...")
        self._status = RuntimeStatus.DRAINING
        await self.remove_replicas(len(self.replicas))
        self._status = RuntimeStatus.IDLE
        logging.info(f"Drain ProcessorPool({self.processor.processor_id}) complete.")
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
            provider=ProviderInfo(
                provider_type=type(source_provider), provider_config={}
            ),
        )
        sink_provider: PushProvider = self.processor.sink().provider()
        sink_info = SinkInfo(
            provider=ProviderInfo(provider_type=type(sink_provider), provider_config={})
        )

        replica_info_list = []
        for replica_actor, _ in list(self.replicas.values()):
            replica_snapshot = await replica_actor.snapshot.remote()
            replica_info_list.append(replica_snapshot)

        actor_info = RayActorInfo(num_cpus=self.config.num_cpus)

        return ProcessorSnapshot(
            processor_id=self.processor.processor_id,
            source=source_info,
            sink=sink_info,
            replicas=replica_info_list,
            actor_info=actor_info,
        )
