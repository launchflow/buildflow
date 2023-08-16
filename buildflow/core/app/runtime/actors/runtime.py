# import asyncio
# from concurrent.futures import Future
import asyncio
import dataclasses
import logging
import time
from datetime import datetime, timedelta
from typing import Iterable, List

import ray
from ray.actor import ActorHandle
from ray.exceptions import OutOfMemoryError, RayActorError

from buildflow.core import utils
from buildflow.core.app.runtime._runtime import RunID, Runtime, RuntimeStatus, Snapshot
from buildflow.core.app.runtime.actors.collector_pattern.collector_pool import (
    CollectorProcessorPoolActor,
)
from buildflow.core.app.runtime.actors.endpoint_pattern.endpoint_pool import (
    EndpointProcessorPoolActor,
)
from buildflow.core.app.runtime.actors.pipeline_pattern.pipeline_pool import (
    PipelineProcessorReplicaPoolActor,
)
from buildflow.core.app.runtime.actors.process_pool import ProcessorSnapshot
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
        self._processor_pool_refs: List[ProcessPoolReference] = []
        self._runtime_loop_future = None

    def _set_status(self, status: RuntimeStatus):
        self._status = status

    def _start_processor(self, processor: ProcessorAPI):
        processor_options = self.options.processor_options[processor.processor_id]
        if processor.processor_type == ProcessorType.PIPELINE:
            processor_pool_ref = ProcessPoolReference(
                actor_handle=PipelineProcessorReplicaPoolActor.remote(
                    self.run_id,
                    processor,
                    processor_options,
                ),
                processor=processor,
            )
        elif processor.processor_type == ProcessorType.COLLECTOR:
            processor_pool_ref = ProcessPoolReference(
                actor_handle=CollectorProcessorPoolActor.remote(
                    self.run_id,
                    processor,
                    processor_options,
                ),
                processor=processor,
            )
        elif processor.processor_type == ProcessorType.CONNECTION:
            raise NotImplementedError("Connection Processors are not yet supported.")
        elif processor.processor_type == ProcessorType.ENDPOINT:
            processor_pool_ref = ProcessPoolReference(
                actor_handle=EndpointProcessorPoolActor.remote(
                    self.run_id, processor, processor_options
                ),
                processor=processor,
            )
        else:
            raise ValueError(f"Unknown ProcessorType: {processor.processor_type}")
        processor_pool_ref.actor_handle.run.remote()
        return processor_pool_ref

    async def run(self, *, processors: Iterable[ProcessorAPI]):
        logging.info("Starting Runtime...")
        if self._status != RuntimeStatus.IDLE:
            raise RuntimeError("Can only start an Idle Runtime.")
        self._set_status(RuntimeStatus.RUNNING)
        self._processor_pool_refs = []
        for processor in processors:
            # TODO: these can fail when the converter isn't provided correctly.
            # i.e. a user provides a type that we don't know how to convert for a source
            # or sink. Right now we just log the error but keep trying.
            process_pool_ref = self._start_processor(processor)
            self._processor_pool_refs.append(process_pool_ref)

        self._runtime_loop_future = self._runtime_checkin_loop()

    async def drain(self) -> bool:
        if self._status == RuntimeStatus.DRAINING:
            logging.warning("Received drain single twice. Killing remaining actors.")
            [
                ray.kill(processor_pool.actor_handle)
                for processor_pool in self._processor_pool_refs
            ]
            # Kill the runtime actor to stop the even loop.
            ray.actor.exit_actor()
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

    async def _runtime_checkin_loop(self):
        logging.info("Runtime checkin loop started...")
        last_autoscale_event = time.monotonic()
        # We keep running the loop while the job is running or draining to ensure
        # we don't exit the main process before the drain is complete.
        # TODO: consider splitting these into two loops, this might be nice as not all
        # processor types need scaling (e.g. only pipeline).
        #   - one for checking the status (i.e. is it still running)
        #   - one for autoscaling
        while (
            self._status == RuntimeStatus.RUNNING
            or self._status == RuntimeStatus.DRAINING
        ):
            scaling_coros = []
            for processor_pool in self._processor_pool_refs:
                try:
                    # Check to see if our processpool actor needs to be restarted.
                    await processor_pool.actor_handle.status.remote()
                except (RayActorError, OutOfMemoryError):
                    logging.exception("process actor unexpectedly died. will restart.")
                    if self._status == RuntimeStatus.RUNNING:
                        # Only restart if we are running, otherwise we are draining
                        new_processor_ref = self._start_processor(
                            processor_pool.processor
                        )
                        processor_pool.actor_handle = new_processor_ref.actor_handle
                processor_options = self.options.processor_options[
                    processor_pool.processor.processor_id
                ]
                autoscale_frequency = timedelta(
                    seconds=processor_options.autoscaler_options.autoscale_frequency_secs
                )
                # Only run the autoscale loop when the runtime is running, this prevents
                # us from scaling while we are draining.
                if self._status == RuntimeStatus.RUNNING and (
                    time.monotonic() - last_autoscale_event
                    >= autoscale_frequency.total_seconds()
                ):
                    logging.debug("Starting autoscale check at: %s", datetime.utcnow())
                    scaling_coros.append(processor_pool.actor_handle.scale.remote())
                    last_autoscale_event = time.monotonic()
            if scaling_coros:
                # TODO: add a try catch here
                try:
                    await asyncio.gather(*scaling_coros)
                except Exception:
                    logging.exception("autoscale failed")
                logging.debug("autoscale check ended at: %s", datetime.utcnow())

            await asyncio.sleep(self.options.checkin_frequency_loop_secs)
