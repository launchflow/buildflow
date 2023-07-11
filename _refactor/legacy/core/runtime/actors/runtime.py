# import asyncio
# from concurrent.futures import Future
import asyncio
import dataclasses
import logging
from typing import Iterable, List, Optional

import ray

from buildflow.api import RuntimeAPI, RuntimeStatus, Snapshot, SnapshotSummary
from buildflow.core import utils
from buildflow.core.processor.base import Processor
from buildflow.core.runtime.actors.process_pool import (
    ProcessorReplicaPoolActor,
    ProcessorSnapshot,
    ProcessorSnapshotSummary,
)
from buildflow.core.runtime.autoscaler import calculate_target_num_replicas
from buildflow.core.runtime.options import RuntimeOptions


@dataclasses.dataclass
class RuntimeSnapshotSummary(SnapshotSummary):
    status: RuntimeStatus
    timestamp_millis: int
    processors: List[ProcessorSnapshotSummary]

    def as_dict(self):
        return {
            "status": self.status.name,
            "timestamp_millis": self.timestamp_millis,
            "processors": {
                processor_summary.processor_id: processor_summary
                for processor_summary in self.processors
            },
        }


@dataclasses.dataclass
class RuntimeSnapshot(Snapshot):
    # TODO(nit): I dont like this name, but I'm not sure what else to call it.
    processors: List[ProcessorSnapshot]

    _timestamp_millis: int = dataclasses.field(default_factory=utils.timestamp_millis)

    def get_timestamp_millis(self) -> int:
        return self._timestamp_millis

    def as_dict(self):
        return {
            "status": self.status.name,
            "timestamp_millis": self._timestamp_millis,
            "processors": [p.as_dict() for p in self.processors],
        }

    def summarize(self) -> RuntimeSnapshotSummary:
        return RuntimeSnapshotSummary(
            status=self.status,
            timestamp_millis=self._timestamp_millis,
            processors=[
                processor_snapshot.summarize() for processor_snapshot in self.processors
            ],
        )


@ray.remote(num_cpus=0.1)
class RuntimeActor(RuntimeAPI):
    def __init__(
        self,
        *,
        runtime_options: RuntimeOptions,
        on_status_change: Optional[callable] = None,
    ) -> None:
        # NOTE: Ray actors run in their own process, so we need to configure
        # logging per actor / remote task.
        logging.getLogger().setLevel(runtime_options.log_level)

        # configuration
        self.options = runtime_options
        self.on_status_change = on_status_change
        # initial runtime state
        self._status = RuntimeStatus.IDLE
        self._processor_pool_actors = []
        self._runtime_loop_future = None

    def _set_status(self, status: RuntimeStatus):
        if self.on_status_change is not None:
            self.on_status_change(status)
        self._status = status

    def run(self, *, processors: Iterable[Processor]):
        logging.info("Starting Runtime...")
        if self._status != RuntimeStatus.IDLE:
            raise RuntimeError("Can only start an Idle Runtime.")
        self._set_status(RuntimeStatus.RUNNING)
        self._processor_pool_actors = []
        for processor in processors:
            # NOTE: the replica configs dictionary is a defaultdict
            replica_config = self.options.replica_options[processor.processor_id]
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
            actor.add_replicas.remote(self.options.num_replicas)

        self._runtime_loop_future = self._runtime_autoscale_loop()

    async def drain(self) -> bool:
        if self._status == RuntimeStatus.DRAINING:
            logging.warning("Received drain single twice. Killing remaining actors.")
            [ray.kill(actor) for actor in self._processor_pool_actors]
        else:
            logging.info("Draining Runtime...")
            logging.info("-- Attempting to drain again will force stop the runtime.")
            self._set_status(RuntimeStatus.DRAINING)
            drain_tasks = [
                actor.drain.remote() for actor in self._processor_pool_actors
            ]
            await asyncio.gather(*drain_tasks)
            self._set_status(RuntimeStatus.IDLE)
            logging.info("Drain Runtime complete.")
        return True

    async def status(self):
        if self._status == RuntimeStatus.DRAINING:
            for actor in self._processor_pool_actors:
                if await actor.status.remote() != RuntimeStatus.IDLE:
                    return RuntimeStatus.DRAINING
            self._set_status(RuntimeStatus.IDLE)
        return self._status

    async def snapshot(self):
        snapshot_tasks = [
            actor.snapshot.remote() for actor in self._processor_pool_actors
        ]
        processor_snapshots = await asyncio.gather(*snapshot_tasks)
        return RuntimeSnapshot(status=self._status, processors=processor_snapshots)

    async def run_until_complete(self):
        if self._runtime_loop_future is not None:
            await self._runtime_loop_future
        self._set_status(RuntimeStatus.IDLE)

    def is_active(self):
        return self._status != RuntimeStatus.IDLE

    async def _runtime_autoscale_loop(self):
        logging.info("Runtime checkin loop started...")
        while self._status == RuntimeStatus.RUNNING:
            for processor_pool in self._processor_pool_actors:
                if self._status != RuntimeStatus.RUNNING:
                    break

                processor_snapshot: ProcessorSnapshot = (
                    await processor_pool.snapshot.remote()
                )
                # Updates the current backlog gauge (metric: ray_current_backlog)
                current_backlog = processor_snapshot.source.backlog
                if current_backlog is None:
                    current_backlog = 0

                current_num_replicas = len(processor_snapshot.replicas)
                # TODO: This is a valid case we need to handle, but this is
                # also happening during initial setup
                if current_num_replicas == 0:
                    continue
                target_num_replicas = calculate_target_num_replicas(
                    processor_snapshot, self.options.autoscaler_options
                )

                num_replicas_delta = target_num_replicas - current_num_replicas
                if num_replicas_delta > 0:
                    processor_pool.add_replicas.remote(num_replicas_delta)
                elif num_replicas_delta < 0:
                    processor_pool.remove_replicas.remote(abs(num_replicas_delta))

            # TODO: Add more control / configuration around the checkin loop
            await asyncio.sleep(30)
