import asyncio
import dataclasses
import logging
from typing import List

import ray
from ray.actor import ActorHandle

from buildflow.core import utils
from buildflow.core.app.runtime._runtime import RunID, Runtime, RuntimeStatus, Snapshot
from buildflow.core.app.runtime.metrics import SimpleGaugeMetric
from buildflow.core.background_tasks.background_task import BackgroundTask
from buildflow.core.options.runtime_options import ProcessorOptions
from buildflow.core.processor.processor import ProcessorAPI, ProcessorID, ProcessorType

ReplicaID = str


@dataclasses.dataclass
class ReplicaReference:
    replica_id: ReplicaID
    ray_actor_handle: ActorHandle


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
        self.initial_replicas = processor_options.autoscaler_options.num_replicas
        self.run_id = run_id
        self.processor = processor
        self.options = processor_options
        # initial runtime state
        self.replicas: List[ReplicaReference] = []
        self.background_tasks: List[BackgroundTask] = self.processor.background_tasks()
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

    async def scale(self):
        raise NotImplementedError("scale must be implemented by subclasses.")

    # NOTE: This method must be implemented by subclasses
    async def create_replica(self):
        raise NotImplementedError("create_replica must be implemented by subclasses.")

    async def add_replicas(self, num_replicas: int):
        if self.status == RuntimeStatus.DRAINING:
            logging.info(
                "cannot add replicas to a darining processor pool."
                "this can happen if a drain occurs at the same time as a scale up."
            )
            return
        if self._status != RuntimeStatus.RUNNING:
            raise RuntimeError(
                "Can only add replicas to a processor pool that is running "
                f"was in state: {self._status}."
            )
        for _ in range(num_replicas):
            replica = await self.create_replica()

            if self._status == RuntimeStatus.RUNNING:
                for _ in range(self.options.num_concurrency):
                    replica.ray_actor_handle.run.remote()
            self.replicas.append(replica)

        self.num_replicas_gauge.set(len(self.replicas))

    async def remove_replicas(self, num_replicas: int):
        if len(self.replicas) < num_replicas:
            raise ValueError(
                f"Cannot remove {num_replicas} replicas from "
                f"{self.processor.processor_id}. Only {len(self.replicas)} replicas "
                "exist."
            )

        actors_to_kill = []
        actor_drain_tasks = []
        for _ in range(num_replicas):
            replica = self.replicas.pop(-1)
            actors_to_kill.append(replica.ray_actor_handle)
            actor_drain_tasks.append(replica.ray_actor_handle.drain.remote())

        if actor_drain_tasks:
            await asyncio.wait(actor_drain_tasks)

        for actor in actors_to_kill:
            ray.kill(actor, no_restart=True)

        self.num_replicas_gauge.set(len(self.replicas))

    async def run(self):
        logging.info(f"Starting ProcessorPool({self.processor.processor_id})...")
        if self._status != RuntimeStatus.IDLE:
            raise RuntimeError("Can only start an Idle Runtime.")
        self._status = RuntimeStatus.RUNNING
        await self.add_replicas(self.initial_replicas)

        coros = []
        for task in self.background_tasks:
            coros.append(task.start())
        await asyncio.gather(*coros)

    async def drain(self):
        logging.info(f"Draining ProcessorPool({self.processor.processor_id})...")
        self._status = RuntimeStatus.DRAINING
        await self.remove_replicas(len(self.replicas))
        coros = []
        for task in self.background_tasks:
            coros.append(task.shutdown())
        await asyncio.gather(*coros)
        self._status = RuntimeStatus.IDLE
        logging.info(f"Drain ProcessorPool({self.processor.processor_id}) complete.")
        return True

    async def status(self):
        if self._status == RuntimeStatus.DRAINING:
            for replica in self.replicas:
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
