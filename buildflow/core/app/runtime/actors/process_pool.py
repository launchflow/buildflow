import asyncio
import dataclasses
import logging
import random
from typing import Dict

import ray
from ray.actor import ActorHandle
from ray.util.placement_group import (
    PlacementGroup,
    remove_placement_group,
)

from buildflow.core import utils
from buildflow.core.app.runtime._runtime import Runtime, RuntimeStatus, Snapshot, RunID
from buildflow.core.app.runtime.metrics import SimpleGaugeMetric
from buildflow.core.options.runtime_options import ProcessorOptions
from buildflow.core.processor.processor import ProcessorAPI, ProcessorID, ProcessorType


@dataclasses.dataclass
class ReplicaReference:
    ray_actor_handle: ActorHandle
    ray_placement_group: PlacementGroup


@dataclasses.dataclass
class ProcessorSnapshot(Snapshot):
    # required snapshot fields
    status: RuntimeStatus
    timestamp_millis: int
    processor_id: ProcessorID
    processor_type: ProcessorType
    num_replicas: float
    num_cpu_per_replica: float
    num_concurrency_per_replica: float

    def as_dict(self) -> dict:
        return {
            "status": self.status.name,
            "timestamp_millis": self.timestamp_millis,
            "processor_id": self.processor_id,
            "processor_type": self.processor_type.name,
            "num_replicas": self.num_replicas,
            "num_cpu_per_replica": self.num_cpu_per_replica,
            "num_concurrency_per_replica": self.num_concurrency_per_replica,
        }


class ProcessorReplicaPoolActor(Runtime):
    """
    This actor acts as a proxy reference for a group of replica Processors.
    Runtime methods are forwarded to the replicas (i.e. 'drain'). Includes
    methods for adding and removing replicas (for autoscaling).
    """

    def __init__(
        self,
        run_id: RunID,
        processor: ProcessorAPI,
        processor_options: ProcessorOptions,
    ) -> None:
        # NOTE: Ray actors run in their own process, so we need to configure
        # logging per actor / remote task.
        logging.getLogger().setLevel(processor_options.log_level)

        # configuration
        self.run_id = run_id
        self.processor = processor
        self.options = processor_options
        # initial runtime state
        self.replicas: Dict[str, ReplicaReference] = {}
        self._status = RuntimeStatus.IDLE
        # metrics
        job_id = ray.get_runtime_context().get_job_id()
        self.num_replicas_gauge = SimpleGaugeMetric(
            "num_replicas",
            description="Current number of replicas. Goes up and down.",
            default_tags={
                "processor_id": self.processor.processor_id,
                "JobId": job_id,
                "RunId": self.run_id,
            },
        )
        self.concurrency_gauge = SimpleGaugeMetric(
            "replica_concurrency",
            description="Current number of concurrency per replica. Goes up and down.",
            default_tags={
                "processor_id": processor.processor_id,
                "JobId": job_id,
                "RunId": self.run_id,
            },
        )
        self.concurrency_gauge.set(self.options.num_concurrency)

    # NOTE: This method must be implemented by subclasses
    async def create_replica(self):
        raise NotImplementedError("create_replica must be implemented by subclasses.")

    async def add_replicas(self, num_replicas: int):
        if self._status != RuntimeStatus.RUNNING:
            raise RuntimeError("Can only replicas to a processor pool that is running.")
        for _ in range(num_replicas):
            replica_id = utils.uuid()
            replica = await self.create_replica()

            if self._status == RuntimeStatus.RUNNING:
                for _ in range(self.options.num_concurrency):
                    replica.ray_actor_handle.run.remote()

            self.replicas[replica_id] = replica

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
            replica = self.replicas.pop(replica_id)
            actors_to_kill.append(replica.ray_actor_handle)
            actor_drain_tasks.append(replica.ray_actor_handle.drain.remote())
            placement_groups_to_remove.append(replica.ray_placement_group)

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
            replica.ray_actor_handle.run.remote()

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
                if await replica.ray_actor_handle.status.remote() != RuntimeStatus.IDLE:
                    return RuntimeStatus.DRAINING
            self._status = RuntimeStatus.IDLE
        return self._status

    # NOTE: Subclasses should override this method if they need to provide additional
    # metrics.
    async def snapshot(self) -> ProcessorSnapshot:
        return ProcessorSnapshot(
            status=self._status,
            timestamp_millis=utils.timestamp_millis(),
            processor_id=self.processor.processor_id,
            processor_type=self.processor.processor_type,
            num_replicas=self.num_replicas_gauge.get_latest_value(),
            num_cpu_per_replica=self.options.num_cpus,
            num_concurrency_per_replica=self.concurrency_gauge.get_latest_value(),
        )
