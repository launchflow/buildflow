# import asyncio
# from concurrent.futures import Future
import asyncio
import dataclasses
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Iterable, List, Optional, Type

import ray
from ray.actor import ActorHandle
from ray.exceptions import OutOfMemoryError, RayActorError

from buildflow.core import utils
from buildflow.core.app.runtime._runtime import (
    RunID,
    Runtime,
    RuntimeEvent,
    RuntimeStatus,
    RuntimeStatusReport,
    Snapshot,
)
from buildflow.core.app.runtime.actors.collector_pattern.collector_pool import (
    CollectorProcessorPoolActor,
)
from buildflow.core.app.runtime.actors.consumer_pattern.consumer_pool import (
    ConsumerProcessorReplicaPoolActor,
)
from buildflow.core.app.runtime.actors.endpoint_pattern.endpoint_pool import (
    EndpointProcessorGroupPoolActor,
)
from buildflow.core.app.runtime.actors.process_pool import ProcessorGroupSnapshot
from buildflow.core.options.runtime_options import RuntimeOptions
from buildflow.core.processor.processor import ProcessorGroup, ProcessorGroupType
from buildflow.dependencies.base import Scope, initialize_dependencies


@dataclasses.dataclass
class RuntimeSnapshot(Snapshot):
    # required snapshot fields
    status: RuntimeStatus
    timestamp_millis: int
    # fields specific to this snapshot class
    processor_groups: List[ProcessorGroupSnapshot]

    def as_dict(self):
        return {
            "status": self.status.name,
            "timestamp_millis": self.timestamp_millis,
            "processor_groups": [p.as_dict() for p in self.processor_groups],
        }


@dataclasses.dataclass
class ProcessorGroupPoolReference:
    actor_handle: ActorHandle
    processor_group: ProcessorGroup


@ray.remote
class RuntimeActor(Runtime):
    def __init__(
        self,
        run_id: RunID,
        *,
        runtime_options: RuntimeOptions,
        flow_dependencies: Dict[Type, Any],
    ) -> None:
        # NOTE: Ray actors run in their own process, so we need to configure
        # logging per actor / remote task.
        logging.getLogger().setLevel(runtime_options.log_level)

        # configuration
        self.run_id = run_id
        self.options = runtime_options
        # initial runtime state
        self._status = RuntimeStatus.PENDING
        self._processor_group_pool_refs: List[ProcessorGroupPoolReference] = []
        self._runtime_loop_future = None
        self.flow_dependencies = flow_dependencies
        self._event_subscriber = None

    def _set_status(self, status: RuntimeStatus):
        self._status = status

    def _start_processor_group(
        self, group: ProcessorGroup, serve_host: str, serve_port: int
    ):
        processor_options = self.options.processor_options[group.group_id]
        if group.group_type == ProcessorGroupType.CONSUMER:
            processor_pool_group_ref = ProcessorGroupPoolReference(
                actor_handle=ConsumerProcessorReplicaPoolActor.remote(
                    self.run_id, group, processor_options, self.flow_dependencies
                ),
                processor_group=group,
            )
        elif group.group_type == ProcessorGroupType.COLLECTOR:
            processor_pool_group_ref = ProcessorGroupPoolReference(
                actor_handle=CollectorProcessorPoolActor.remote(
                    self.run_id,
                    group,
                    processor_options,
                    self.flow_dependencies,
                    serve_host,
                    serve_port,
                ),
                processor_group=group,
            )
        elif group.group_type == ProcessorGroupType.SERVICE:
            processor_pool_group_ref = ProcessorGroupPoolReference(
                actor_handle=EndpointProcessorGroupPoolActor.remote(
                    self.run_id,
                    group,
                    processor_options,
                    self.flow_dependencies,
                    serve_host,
                    serve_port,
                ),
                processor_group=group,
            )
        else:
            raise ValueError(f"Unknown group type: {group.group_type}")
        processor_pool_group_ref.actor_handle.run.remote()
        return processor_pool_group_ref

    async def initialize_global_dependencies(
        self, processor_groups: Iterable[ProcessorGroup]
    ):
        for group in processor_groups:
            deps = []
            for processor in group.processors:
                deps.extend(processor.dependencies())
            await initialize_dependencies(deps, self.flow_dependencies, [Scope.GLOBAL])

    async def run(
        self,
        *,
        processor_groups: Iterable[ProcessorGroup],
        serve_host: str,
        serve_port: int,
        event_subscriber: Optional[Callable],
    ):
        logging.info("Starting Runtime...")
        self._event_subscriber = event_subscriber
        self._set_status(RuntimeStatus.RUNNING)
        self._processor_group_pool_refs = []
        await self.initialize_global_dependencies(processor_groups)
        for processor_group in processor_groups:
            process_group_pool_ref = self._start_processor_group(
                processor_group, serve_host, serve_port
            )
            self._processor_group_pool_refs.append(process_group_pool_ref)

        self._runtime_loop_future = self._runtime_checkin_loop(serve_host, serve_port)

    async def drain(self, as_reload: bool = False) -> bool:
        if (
            self._status == RuntimeStatus.DRAINING
            or self._status == RuntimeStatus.RELOADING
        ):
            logging.warning("Received drain single twice. Killing remaining actors.")
            [
                ray.kill(processor_pool.actor_handle)
                for processor_pool in self._processor_group_pool_refs
            ]
            # Kill the runtime actor to stop the even loop.
            if self._event_subscriber is not None:
                event = RuntimeEvent(
                    self.run_id,
                    RuntimeStatusReport(
                        status=RuntimeStatus.STOPPED,
                        processor_group_statuses={},
                    ),
                )
                self._event_subscriber(event)
            ray.actor.exit_actor()
        else:
            if as_reload:
                logging.warning("Draining Runtime for reload...")
                self._set_status(RuntimeStatus.RELOADING)
            else:
                logging.warning("Draining Runtime...")
                logging.warning(
                    "-- Attempting to drain again will force stop the runtime."
                )
                self._set_status(RuntimeStatus.DRAINING)
            drain_tasks = [
                processor_pool.actor_handle.drain.remote()
                for processor_pool in self._processor_group_pool_refs
            ]
            await asyncio.gather(*drain_tasks)
            if not as_reload:
                logging.info("Drain Runtime complete.")
            self._set_status(RuntimeStatus.DRAINED)
        return True

    async def status(self):
        return self._status

    async def snapshot(self):
        snapshot_tasks = [
            processor_pool.actor_handle.snapshot.remote()
            for processor_pool in self._processor_group_pool_refs
        ]
        processor_snapshots = await asyncio.gather(*snapshot_tasks)
        return RuntimeSnapshot(
            status=self._status,
            timestamp_millis=utils.timestamp_millis(),
            processor_groups=processor_snapshots,
        )

    async def run_until_complete(self):
        if self._runtime_loop_future is not None:
            await self._runtime_loop_future

    async def _runtime_checkin_loop(
        self,
        serve_host: str,
        serve_port: int,
    ):
        logging.info("Runtime checkin loop started...")
        last_autoscale_event = time.monotonic()
        # We keep running the loop while the job is running or draining to ensure
        # we don't exit the main process before the drain is complete.
        # TODO: consider splitting these into two loops, this might be nice as not all
        # processor types need scaling (e.g. only consumer).
        #   - one for checking the status (i.e. is it still running)
        #   - one for autoscaling
        previous_status_report = None
        while (
            self._status == RuntimeStatus.RUNNING
            or self._status == RuntimeStatus.DRAINING
        ):
            scaling_coros = []
            processor_group_statuses = {}
            for processor_pool in self._processor_group_pool_refs:
                try:
                    # Check to see if our processpool actor needs to be restarted.
                    status = await processor_pool.actor_handle.status.remote()
                    processor_group_statuses[
                        processor_pool.processor_group.group_id
                    ] = status
                except (RayActorError, OutOfMemoryError):
                    processor_group_statuses[
                        processor_pool.processor_group.group_id
                    ] = RuntimeStatus.DIED
                    logging.exception("process actor unexpectedly died. will restart.")
                    if self._status == RuntimeStatus.RUNNING:
                        # Only restart if we are running, otherwise we are draining
                        new_processor_ref = self._start_processor_group(
                            processor_pool.processor_group,
                            serve_host=serve_host,
                            serve_port=serve_port,
                        )
                        processor_pool.actor_handle = new_processor_ref.actor_handle
                if self._event_subscriber is not None:
                    status_report = RuntimeStatusReport(
                        status=self._status,
                        processor_group_statuses=processor_group_statuses,
                    )

                    try:
                        if previous_status_report != status_report:
                            event = RuntimeEvent(self.run_id, status_report)
                            self._event_subscriber(event)
                    except Exception:
                        logging.exception("event subscriber failed")
                    previous_status_report = status_report

                processor_options = self.options.processor_options[
                    processor_pool.processor_group.group_id
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
                try:
                    await asyncio.gather(*scaling_coros)
                except Exception:
                    logging.exception("autoscale failed")
                logging.debug("autoscale check ended at: %s", datetime.utcnow())

            await asyncio.sleep(self.options.checkin_frequency_loop_secs)
        processor_group_statuses = {}
        for processor_pool in self._processor_group_pool_refs:
            try:
                # Check to see if our processpool actor needs to be restarted.
                status = await processor_pool.actor_handle.status.remote()
                processor_group_statuses[
                    processor_pool.processor_group.group_id
                ] = status
            except (RayActorError, OutOfMemoryError):
                processor_group_statuses[
                    processor_pool.processor_group.group_id
                ] = RuntimeStatus.DIED
            if self._event_subscriber is not None:
                status_report = RuntimeStatusReport(
                    status=self._status,
                    processor_group_statuses=processor_group_statuses,
                )

                try:
                    event = RuntimeEvent(self.run_id, status_report)
                    self._event_subscriber(event)
                except Exception:
                    logging.exception("event subscriber failed")
