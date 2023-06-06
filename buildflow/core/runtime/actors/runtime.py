# import asyncio
# from concurrent.futures import Future
import asyncio
import dataclasses
import logging
from typing import Iterable, List

import ray
from ray.util.metrics import Gauge

from buildflow import utils
from buildflow.api import RuntimeAPI, RuntimeStatus, Snapshot
from buildflow.core.processor.base import Processor
from buildflow.core.runtime.actors.process_pool import (
    ProcessorReplicaPoolActor,
    ProcessorSnapshot,
)
from buildflow.core.runtime.autoscale import calculate_target_num_replicas
from buildflow.core.runtime.config import RuntimeConfig


@dataclasses.dataclass
class RuntimeSnapshot(Snapshot):
    # TODO(nit): I dont like this name, but I'm not sure what else to call it.
    processors: List[ProcessorSnapshot]

    _timestamp: int = dataclasses.field(default_factory=utils.timestamp_millis)

    def get_timestamp_millis(self) -> int:
        return self._timestamp

    def as_dict(self):
        return dataclasses.asdict(self)


@ray.remote(num_cpus=0.1)
class RuntimeActor(RuntimeAPI):
    def __init__(self, config: RuntimeConfig) -> None:
        # NOTE: Ray actors run in their own process, so we need to configure
        # logging per actor / remote task.
        logging.getLogger().setLevel(config.log_level)

        # configuration
        self.config = config
        # initial runtime state
        self._status = RuntimeStatus.IDLE
        self._processor_pool_actors = []
        self._runtime_loop_future = None
        # metrics
        job_id = ray.get_runtime_context().get_job_id()
        self.current_backlog_gauge = Gauge(
            "current_backlog",
            description="Current backlog of the actor. Goes up and down.",
            tag_keys=(
                "processor_id",
                "JobId",
            ),
        )
        self.current_backlog_gauge.set_default_tags(
            {
                # In the case where we could not get the processor name
                "processor_id": "unknown",
                "JobId": job_id,
            }
        )

    def run(self, *, processors: Iterable[Processor]):
        logging.info("Starting Runtime...")
        if self._status != RuntimeStatus.IDLE:
            raise RuntimeError("Can only start an Idle Runtime.")
        self._status = RuntimeStatus.RUNNING
        self._processor_pool_actors = []
        for processor in processors:
            # NOTE: the replica configs dictionary is a defaultdict
            replica_config = self.config.replica_configs[processor.processor_id]
            self._processor_pool_actors.append(
                ProcessorReplicaPoolActor.remote(processor, replica_config)
            )

        # TODO: these can fail sometimes when the converter isn't provided correctly.
        # i.e. a user provides a type that we don't know how to convert for a source /
        # sink. Right now we just log the error but keep trying.
        for actor in self._processor_pool_actors:
            # Ensure we can start the actor. This might fail if the processor is
            # misconfigured.
            actor.run.remote()
            actor.add_replicas.remote(self.config.num_replicas)

        self._runtime_loop_future = self._runtime_checkin_loop()

    async def drain(self) -> bool:
        logging.info("Draining Runtime...")
        self._status = RuntimeStatus.DRAINING
        drain_tasks = [actor.drain.remote() for actor in self._processor_pool_actors]
        await asyncio.gather(*drain_tasks)
        self._status = RuntimeStatus.IDLE
        logging.info("Drain Runtime complete.")
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
        return RuntimeSnapshot(processors=processor_snapshots)

    async def run_until_complete(self):
        if self._runtime_loop_future is not None:
            await self._runtime_loop_future

    def is_active(self):
        return self._status != RuntimeStatus.IDLE

    async def _runtime_checkin_loop(self):
        logging.info("Runtime checkin loop started...")
        while self._status == RuntimeStatus.RUNNING:
            for processor_pool in self._processor_pool_actors:
                if self._status != RuntimeStatus.RUNNING:
                    break

                processor_snapshot = await processor_pool.snapshot.remote()
                # Updates the current backlog gauge (metric: ray_current_backlog)
                current_backlog = processor_snapshot.source.backlog
                if current_backlog is None:
                    current_backlog = 0
                self.current_backlog_gauge.set(
                    current_backlog,
                    tags={
                        # set the processor name to index the metric by
                        "processor_id": processor_snapshot.processor_id
                    },
                )

                current_num_replicas = len(processor_snapshot.replicas)
                # TODO: This is a valid case we need to handle, but this is
                # also happening during initial setup
                if current_num_replicas == 0:
                    continue
                target_num_replicas = calculate_target_num_replicas(
                    processor_snapshot, self.config.autoscaler_config
                )

                num_replicas_delta = target_num_replicas - current_num_replicas
                if num_replicas_delta > 0:
                    processor_pool.add_replicas.remote(num_replicas_delta)
                elif num_replicas_delta < 0:
                    processor_pool.remove_replicas.remote(abs(num_replicas_delta))

            # TODO: Add more control / configuration around the checkin loop
            await asyncio.sleep(30)
