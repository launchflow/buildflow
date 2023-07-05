# import asyncio
# from concurrent.futures import Future
import asyncio
import dataclasses
import logging
from typing import Iterable, List

import ray

from buildflow.core.io.providers._provider import ProcessorProvider
from buildflow.core.app.runtime._runtime import Runtime, RuntimeStatus, Snapshot
from buildflow.core.app.runtime.actors.process_pool import (
    ProcessorReplicaPoolActor,
    ProcessorSnapshot,
)
from buildflow.core.app.runtime.autoscaler import calculate_target_num_replicas
from buildflow.core.options.runtime_options import RuntimeOptions
from buildflow import utils


@dataclasses.dataclass
class RuntimeSnapshot(Snapshot):
    # required snapshot fields
    status: RuntimeStatus
    timestamp_millis: int
    # fields specific to this snapshot class
    processors: List[ProcessorSnapshot]

    def as_dict(self):
        return {
            "status": self.status.name,
            "timestamp_millis": self.timestamp_millis,
            "processors": [p.as_dict() for p in self.processors],
        }


@ray.remote(num_cpus=0.1)
class RuntimeActor(Runtime):
    def __init__(
        self,
        *,
        runtime_options: RuntimeOptions,
    ) -> None:
        # NOTE: Ray actors run in their own process, so we need to configure
        # logging per actor / remote task.
        logging.getLogger().setLevel(runtime_options.log_level)

        # configuration
        self.options = runtime_options
        # initial runtime state
        self._status = RuntimeStatus.IDLE
        self._processor_pool_actors = []
        self._runtime_loop_future = None

    def _set_status(self, status: RuntimeStatus):
        self._status = status

    def run(self, *, processors: Iterable[ProcessorProvider]):
        logging.info("Starting Runtime...")
        if self._status != RuntimeStatus.IDLE:
            raise RuntimeError("Can only start an Idle Runtime.")
        self._set_status(RuntimeStatus.RUNNING)
        self._processor_pool_actors = []
        for processor in processors:
            processor_options = self.options.processor_options[processor.processor_id]
            self._processor_pool_actors.append(
                ProcessorReplicaPoolActor.remote(processor, processor_options)
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
        return RuntimeSnapshot(
            status=self._status,
            timestamp_millis=utils.timestamp_millis(),
            processors=processor_snapshots,
        )

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
                current_backlog = processor_snapshot.source_backlog
                if current_backlog is None:
                    current_backlog = 0

                current_num_replicas = processor_snapshot.num_replicas
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
