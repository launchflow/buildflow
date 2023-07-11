import asyncio
import dataclasses
import logging
import random
from typing import Dict, List, Optional

import ray
from ray.util.placement_group import placement_group, remove_placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

from buildflow.api import ProcessorID, RuntimeAPI, RuntimeStatus, Snapshot
from buildflow.api.runtime import SnapshotSummary
from buildflow.core import utils
from buildflow.core.processor.base import Processor
from buildflow.core.runtime.actors.pull_process_push import (
    PullProcessPushActor,
    PullProcessPushSnapshot,
)
from buildflow.core.runtime.metrics import RateCalculation, SimpleGaugeMetric
from buildflow.core.runtime.options import ReplicaOptions
from buildflow.resources.io.providers.base import SinkProvider, SourceProvider

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


# Should we have ProviderAPI metrics / snapshots?
@dataclasses.dataclass
class SinkInfo:
    provider: ProviderInfo


@dataclasses.dataclass
class RayActorInfo:
    num_cpus: float


@dataclasses.dataclass
class ProcessorSnapshotSummary(SnapshotSummary):
    status: RuntimeStatus
    timestamp_millis: int
    processor_id: ProcessorID
    source_backlog: float
    num_replicas: float
    num_concurrency_per_replica: float
    total_events_processed_per_sec: float
    total_pulls_per_sec: float
    avg_num_elements_per_batch: float
    avg_pull_percentage_per_replica: float
    avg_process_time_millis_per_element: float
    avg_process_time_millis_per_batch: float
    avg_pull_to_ack_time_millis_per_batch: float

    def as_dict(self) -> dict:
        return {
            "status": self.status.name,
            "timestamp_millis": self.timestamp_millis,
            "processor_id": self.processor_id,
            "source_backlog": self.source_backlog,
            "num_replicas": self.num_replicas,
            "num_concurrency_per_replica": self.num_concurrency_per_replica,
            "total_events_processed_per_sec": self.total_events_processed_per_sec,
            "total_pulls_per_sec": self.total_pulls_per_sec,
            "avg_num_elements_per_batch": self.avg_num_elements_per_batch,
            "avg_pull_percentage_per_replica": self.avg_pull_percentage_per_replica,
            "avg_process_time_millis_per_element": self.avg_process_time_millis_per_element,  # noqa: E501
            "avg_process_time_millis_per_batch": self.avg_process_time_millis_per_batch,  # noqa: E501
            "avg_pull_to_ack_time_millis_per_batch": self.avg_pull_to_ack_time_millis_per_batch,  # noqa: E501
        }


@dataclasses.dataclass
class ProcessorSnapshot(Snapshot):
    processor_id: ProcessorID
    source: SourceInfo
    sink: SinkInfo
    replicas: List[PullProcessPushSnapshot]
    actor_info: RayActorInfo
    # metrics
    # NOTE: backlog is also stored in SourceInfo, but might delete it.
    source_backlog: float
    num_replicas: float
    num_concurrency_per_replica: float
    # private fields
    _timestamp_millis: int = dataclasses.field(default_factory=utils.timestamp_millis)

    def get_timestamp_millis(self) -> int:
        return self._timestamp_millis

    def as_dict(self) -> dict:
        return {
            "status": self.status.name,
            "timestamp_millis": self._timestamp_millis,
            "processor_id": self.processor_id,
            "source": dataclasses.asdict(self.source),
            "sink": dataclasses.asdict(self.sink),
            "replicas": [r.as_dict() for r in self.replicas],
            "actor_info": dataclasses.asdict(self.actor_info),
            "source_backlog": self.source_backlog,
            "num_replicas": self.num_replicas,
            "num_concurrency_per_replica": self.num_concurrency_per_replica,
        }

    def summarize(self) -> ProcessorSnapshotSummary:
        # below metric(s) derived from the `events_processed_per_sec` composite counter
        total_events_processed_per_sec = RateCalculation.merge(
            [
                replica_snapshot.events_processed_per_sec
                for replica_snapshot in self.replicas
            ]
        ).total_value_rate()
        avg_num_elements_per_batch = RateCalculation.merge(
            [
                replica_snapshot.events_processed_per_sec
                for replica_snapshot in self.replicas
            ]
        ).average_value_rate()
        # below metric(s) derived from the `pull_percentage` composite counter
        total_pulls_per_sec = RateCalculation.merge(
            [replica_snapshot.pull_percentage for replica_snapshot in self.replicas]
        ).total_count_rate()
        avg_pull_percentage_per_replica = RateCalculation.merge(
            [replica_snapshot.pull_percentage for replica_snapshot in self.replicas]
        ).average_value_rate()
        # below metric(s) derived from the `process_time_millis` composite counter
        avg_process_time_millis_per_element = RateCalculation.merge(
            [replica_snapshot.process_time_millis for replica_snapshot in self.replicas]
        ).average_value_rate()
        # below metric(s) derived from the `process_batch_time_millis` composite counter
        avg_process_time_millis_per_batch = RateCalculation.merge(
            [
                replica_snapshot.process_batch_time_millis
                for replica_snapshot in self.replicas
            ]
        ).average_value_rate()
        # below metric(s) derived from the `pull_to_ack_time_millis` composite counter
        avg_pull_to_ack_time_millis_per_batch = RateCalculation.merge(
            [
                replica_snapshot.pull_to_ack_time_millis
                for replica_snapshot in self.replicas
            ]
        ).average_value_rate()

        return ProcessorSnapshotSummary(
            status=self.status,
            timestamp_millis=self._timestamp_millis,
            processor_id=self.processor_id,
            source_backlog=self.source_backlog,
            num_replicas=self.num_replicas,
            num_concurrency_per_replica=self.num_concurrency_per_replica,
            total_events_processed_per_sec=total_events_processed_per_sec,
            avg_num_elements_per_batch=avg_num_elements_per_batch,
            total_pulls_per_sec=total_pulls_per_sec,
            avg_pull_percentage_per_replica=avg_pull_percentage_per_replica,
            avg_process_time_millis_per_element=avg_process_time_millis_per_element,
            avg_process_time_millis_per_batch=avg_process_time_millis_per_batch,  # noqa: E501
            avg_pull_to_ack_time_millis_per_batch=avg_pull_to_ack_time_millis_per_batch,  # noqa: E501
        )


@ray.remote(num_cpus=0.1)
class ProcessorReplicaPoolActor(RuntimeAPI):
    """
    This actor acts as a proxy reference for a group of replica Processors.
    Runtime methods are forwarded to the replicas (i.e. 'drain'). Includes
    methods for adding and removing replicas (for autoscaling).
    """

    def __init__(self, processor: Processor, config: ReplicaOptions) -> None:
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
        self.num_replicas_gauge = SimpleGaugeMetric(
            "num_replicas",
            description="Current number of replicas. Goes up and down.",
            default_tags={
                "processor_id": self.processor.processor_id,
                "JobId": job_id,
            },
        )
        self.concurrency_gauge = SimpleGaugeMetric(
            "ppp_concurrency",
            description="Current number of concurrency per replica. Goes up and down.",
            default_tags={
                "processor_id": processor.processor_id,
                "JobId": job_id,
            },
        )
        self.concurrency_gauge.set(config.num_concurrency)
        self.current_backlog_gauge = SimpleGaugeMetric(
            "current_backlog",
            description="Current backlog of the actor. Goes up and down.",
            default_tags={
                "processor_id": processor.processor_id,
                "JobId": job_id,
            },
        )

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
                for _ in range(self.config.num_concurrency):
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
        source_provider: SourceProvider = self.processor.source().provider()
        source_backlog = await source_provider.backlog()
        # Log the current backlog so ray metrics can pick it up
        if source_backlog is not None:
            self.current_backlog_gauge.set(source_backlog)
        source_info = SourceInfo(
            backlog=source_backlog,
            provider=ProviderInfo(
                provider_type=source_provider.__class__.__name__, provider_config={}
            ),
        )
        sink_provider: SinkProvider = self.processor.sink().provider()
        sink_info = SinkInfo(
            provider=ProviderInfo(
                provider_type=sink_provider.__class__.__name__, provider_config={}
            )
        )

        replica_info_list = []
        for replica_actor, _ in list(self.replicas.values()):
            replica_snapshot = await replica_actor.snapshot.remote()
            replica_info_list.append(replica_snapshot)

        actor_info = RayActorInfo(num_cpus=self.config.num_cpus)

        return ProcessorSnapshot(
            status=self._status,
            processor_id=self.processor.processor_id,
            source=source_info,
            sink=sink_info,
            replicas=replica_info_list,
            actor_info=actor_info,
            source_backlog=self.current_backlog_gauge.get_latest_value(),
            num_replicas=self.num_replicas_gauge.get_latest_value(),
            num_concurrency_per_replica=self.concurrency_gauge.get_latest_value(),
        )
