# import asyncio
# from concurrent.futures import Future
import asyncio
import dataclasses
from datetime import datetime, timedelta
import logging
from typing import Iterable, List

import ray
from ray.actor import ActorHandle
from ray.exceptions import RayActorError

from buildflow.core import utils
from buildflow.core.app.runtime._runtime import Runtime, RuntimeStatus, Snapshot, RunID
from buildflow.core.app.runtime.actors.pipeline_pattern.pipeline_pool import (
    PipelineProcessorReplicaPoolActor,
)
from buildflow.core.app.runtime.actors.process_pool import (
    ProcessorSnapshot,
)
from buildflow.core.app.runtime.autoscaler import calculate_target_num_replicas
from buildflow.core.options.runtime_options import RuntimeOptions
from buildflow.core.processor.processor import ProcessorAPI, ProcessorType


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


@dataclasses.dataclass
class ProcessPoolReference:
    actor_handle: ActorHandle
    processor: ProcessorAPI


@ray.remote(num_cpus=0.1)
class RuntimeActor(Runtime):
    def __init__(
        self,
        run_id: RunID,
        *,
        runtime_options: RuntimeOptions,
    ) -> None:
        # NOTE: Ray actors run in their own process, so we need to configure
        # logging per actor / remote task.
        logging.getLogger().setLevel(runtime_options.log_level)

        # configuration
        self.run_id = run_id
        self.options = runtime_options
        # initial runtime state
        self._status = RuntimeStatus.IDLE
        self._processor_pool_refs = []
        self._runtime_loop_future = None
        self._autoscale_frequency = timedelta(
            seconds=self.options.autoscaler_options.autoscale_frequency_secs
        )

    def _set_status(self, status: RuntimeStatus):
        self._status = status

    async def run(self, *, processors: Iterable[ProcessorAPI]):
        logging.info("Starting Runtime...")
        if self._status != RuntimeStatus.IDLE:
            raise RuntimeError("Can only start an Idle Runtime.")
        self._set_status(RuntimeStatus.RUNNING)
        self._processor_pool_refs = []
        for processor in processors:
            processor_options = self.options.processor_options[processor.processor_id]
            if processor.processor_type == ProcessorType.PIPELINE:
                self._processor_pool_refs.append(
                    ProcessPoolReference(
                        actor_handle=PipelineProcessorReplicaPoolActor.remote(
                            self.run_id, processor, processor_options
                        ),
                        processor=processor,
                    )
                )
            elif processor.processor_type == ProcessorType.COLLECTOR:
                raise NotImplementedError("Collector Processors are not yet supported.")
            elif processor.processor_type == ProcessorType.CONNECTION:
                raise NotImplementedError(
                    "Connection Processors are not yet supported."
                )
            elif processor.processor_type == ProcessorType.SERVICE:
                raise NotImplementedError("Service Processors are not yet supported.")
            else:
                raise ValueError(f"Unknown ProcessorType: {processor.processor_type}")

        # TODO: these can fail sometimes when the converter isn't provided correctly.
        # i.e. a user provides a type that we don't know how to convert for a source /
        # sink. Right now we just log the error but keep trying.
        for processor_pool in self._processor_pool_refs:
            # Ensure we can start the actor. This might fail if the processor is
            # misconfigured.
            processor_pool.actor_handle.run.remote()
            await processor_pool.actor_handle.add_replicas.remote(
                self.options.num_replicas
            )

        self._runtime_loop_future = self._runtime_autoscale_loop()

    async def drain(self) -> bool:
        if self._status == RuntimeStatus.DRAINING:
            logging.warning("Received drain single twice. Killing remaining actors.")
            [
                ray.kill(processor_pool.actor_handle)
                for processor_pool in self._processor_pool_refs
            ]
        else:
            logging.warning("Draining Runtime...")
            logging.warning("-- Attempting to drain again will force stop the runtime.")
            self._set_status(RuntimeStatus.DRAINING)
            drain_tasks = [
                processor_pool.actor_handle.drain.remote()
                for processor_pool in self._processor_pool_refs
            ]
            await asyncio.gather(*drain_tasks)
            self._set_status(RuntimeStatus.IDLE)
            logging.info("Drain Runtime complete.")
        return True

    async def status(self):
        if self._status == RuntimeStatus.DRAINING:
            for processor_pool in self._processor_pool_refs:
                if (
                    await processor_pool.actor_handle.status.remote()
                    != RuntimeStatus.IDLE
                ):
                    return RuntimeStatus.DRAINING
            self._set_status(RuntimeStatus.IDLE)
        return self._status

    async def snapshot(self):
        snapshot_tasks = [
            processor_pool.actor_handle.snapshot.remote()
            for processor_pool in self._processor_pool_refs
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
        last_autoscale_event = datetime.utcnow()
        # We keep running the loop while the job is running or draining to ensure
        # we don't exit the main process before the drain is complete.
        # TODO: consider splitting these into two loops.
        #   - one for checking the status (i.e. is it still running)
        #   - one for autoscaling
        while (
            self._status == RuntimeStatus.RUNNING
            or self._status == RuntimeStatus.DRAINING
        ):
            # Only run the autoscale loop when the runtime is running, this prevents
            # us from scaling while we are draining.
            if self._status == RuntimeStatus.RUNNING and (
                datetime.utcnow() - last_autoscale_event >= self._autoscale_frequency
            ):
                last_autoscale_event = datetime.utcnow()
                for processor_pool in self._processor_pool_refs:
                    if self._status != RuntimeStatus.RUNNING:
                        break

                    try:
                        processor_snapshot: ProcessorSnapshot = (
                            await processor_pool.actor_handle.snapshot.remote()
                        )
                    except RayActorError:
                        logging.exception(
                            "process actor unexpectedly died. will restart."
                        )
                        options = self.options.processor_options[
                            processor_pool.processor.processor_id
                        ]
                        processor_pool.actor_handle = (
                            PipelineProcessorReplicaPoolActor.remote(
                                self.run_id,
                                processor_pool.processor,
                                options,
                            )
                        )
                        # Restart with the default number of replicas, and let autoscale
                        # handle the rest.
                        processor_pool.actor_handle.run.remote()
                        await processor_pool.actor_handle.add_replicas.remote(
                            self.options.num_replicas
                        )
                        continue
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
                        processor_pool.actor_handle.add_replicas.remote(
                            num_replicas_delta
                        )
                    elif num_replicas_delta < 0:
                        processor_pool.actor_handle.remove_replicas.remote(
                            abs(num_replicas_delta)
                        )

            await asyncio.sleep(self.options.checkin_frequency_loop_secs)
