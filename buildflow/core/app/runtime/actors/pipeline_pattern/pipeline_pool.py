import logging
from collections import deque
from typing import List

import ray
from ray.exceptions import OutOfMemoryError, RayActorError

from buildflow.core import utils
from buildflow.core.app.runtime._runtime import RunID, RuntimeStatus
from buildflow.core.app.runtime.actors.pipeline_pattern.pipeline_pool_snapshot import (
    PipelineProcessorSnapshot,
)
from buildflow.core.app.runtime.actors.pipeline_pattern.pull_process_push import (
    PullProcessPushActor,
    PullProcessPushSnapshot,
)
from buildflow.core.app.runtime.actors.process_pool import (
    ProcessorReplicaPoolActor,
    ProcessorSnapshot,
    ReplicaReference,
)
from buildflow.core.app.runtime.autoscaler import calculate_target_num_replicas
from buildflow.core.app.runtime.metrics import RateCalculation, SimpleGaugeMetric
from buildflow.core.options.runtime_options import ProcessorOptions
from buildflow.core.processor.patterns.pipeline import PipelineProcessor


@ray.remote(num_cpus=0.1)
class PipelineProcessorReplicaPoolActor(ProcessorReplicaPoolActor):
    """
    This actor acts as a proxy reference for a group of replica Processors.
    Runtime methods are forwarded to the replicas (i.e. 'drain'). Includes
    methods for adding and removing replicas (for autoscaling).
    """

    # TODO: Add a PipelineOptions type for pipeline-specific options
    def __init__(
        self,
        run_id: RunID,
        processor: PipelineProcessor,
        processor_options: ProcessorOptions,
    ) -> None:
        super().__init__(run_id, processor, processor_options)
        # NOTE: Ray actors run in their own process, so we need to configure
        # logging per actor / remote task.
        logging.getLogger().setLevel(processor_options.log_level)

        # configuration
        self.processor = processor
        self.source = processor.source()
        self.options = processor_options
        # metrics
        job_id = ray.get_runtime_context().get_job_id()
        self.current_backlog_gauge = SimpleGaugeMetric(
            "current_backlog",
            description="Current backlog of the actor. Goes up and down.",
            default_tags={
                "processor_id": processor.processor_id,
                "JobId": job_id,
                "RunId": self.run_id,
            },
        )
        self.prev_snapshot: ProcessorSnapshot = None

    async def scale(self):
        if self._status != RuntimeStatus.RUNNING:
            return
        processor_snapshot: ProcessorSnapshot = await self.snapshot()
        if processor_snapshot.num_replicas == 0:
            # This can happen if all actors unexpectedly die.
            # Just restart with what the user initiall requested, and scale
            # up from there.
            await self.add_replicas(self.options.autoscaler_options.num_replicas)
            return processor_snapshot
        # Updates the current backlog gauge (metric: ray_current_backlog)
        current_backlog = processor_snapshot.source_backlog
        if current_backlog is None:
            current_backlog = 0

        current_num_replicas = processor_snapshot.num_replicas
        target_num_replicas = calculate_target_num_replicas(
            current_snapshot=processor_snapshot,
            prev_snapshot=self.prev_snapshot,
            config=self.options.autoscaler_options,
        )

        num_replicas_delta = target_num_replicas - current_num_replicas
        if num_replicas_delta > 0:
            await self.add_replicas(num_replicas_delta)
        elif num_replicas_delta < 0:
            await self.remove_replicas(abs(num_replicas_delta))
        self.prev_snapshot = processor_snapshot

    # NOTE: Providing this method is the main purpose of this class. It allows us to
    # contain any runtime logic that applies to all Processor types.
    async def create_replica(self) -> ReplicaReference:
        replica_id = utils.uuid()
        replica_actor_handle = PullProcessPushActor.options(
            num_cpus=self.options.num_cpus,
        ).remote(
            self.run_id,
            self.processor,
            replica_id=replica_id,
            log_level=self.options.log_level,
        )

        return ReplicaReference(
            replica_id=replica_id,
            ray_actor_handle=replica_actor_handle,
        )

    async def snapshot(self) -> PipelineProcessorSnapshot:
        source_backlog = await self.source.backlog()
        # Log the current backlog so ray metrics can pick it up
        self.current_backlog_gauge.set(source_backlog)

        replica_snapshots: List[PullProcessPushSnapshot] = []
        # TODO: Dont access self.replicas directly. It should be accessed via a method
        # interface
        dead_replica_indices = deque()
        for i, replica in enumerate(self.replicas):
            try:
                snapshot: PullProcessPushSnapshot = (
                    await replica.ray_actor_handle.snapshot.remote()
                )
                replica_snapshots.append(snapshot)
            except (RayActorError, OutOfMemoryError):
                logging.exception("replica actor unexpectedly died. will restart.")
                # We keep this list reverse sorted so we can iterate and remove
                dead_replica_indices.appendleft(i)
        for idx in dead_replica_indices:
            replica = self.replicas.pop(idx)
        if dead_replica_indices:
            logging.error("removed %s dead replicas", len(dead_replica_indices))
            # update our gauge if had to remove some replicas.
            self.num_replicas_gauge.set(len(self.replicas))
        # NOTE: we grab the parrent snapshot after we've updated the replica list
        # this ensure we don't include dead replicas
        parent_snapshot: ProcessorSnapshot = await super().snapshot()
        # below metric(s) derived from the `events_processed_per_sec` composite counter
        total_events_processed_per_sec = RateCalculation.merge(
            [
                replica_snapshot.events_processed_per_sec
                for replica_snapshot in replica_snapshots
            ]
        ).total_value_rate()
        avg_num_elements_per_batch = RateCalculation.merge(
            [
                replica_snapshot.events_processed_per_sec
                for replica_snapshot in replica_snapshots
            ]
        ).average_value_rate()
        # below metric(s) derived from the `pull_percentage` composite counter
        total_pulls_per_sec = RateCalculation.merge(
            [replica_snapshot.pull_percentage for replica_snapshot in replica_snapshots]
        ).total_count_rate()
        avg_pull_percentage_per_replica = RateCalculation.merge(
            [replica_snapshot.pull_percentage for replica_snapshot in replica_snapshots]
        ).average_value_rate()
        # below metric(s) derived from the `process_time_millis` composite counter
        avg_process_time_millis_per_element = RateCalculation.merge(
            [
                replica_snapshot.process_time_millis
                for replica_snapshot in replica_snapshots
            ]
        ).average_value_rate()
        # below metric(s) derived from the `process_batch_time_millis` composite counter
        avg_process_time_millis_per_batch = RateCalculation.merge(
            [
                replica_snapshot.process_batch_time_millis
                for replica_snapshot in replica_snapshots
            ]
        ).average_value_rate()
        # below metric(s) derived from the `pull_to_ack_time_millis` composite counter
        avg_pull_to_ack_time_millis_per_batch = RateCalculation.merge(
            [
                replica_snapshot.pull_to_ack_time_millis
                for replica_snapshot in replica_snapshots
            ]
        ).average_value_rate()

        # below metrics(s) derived from the `cpu_percentage` composite counter
        avg_cpu_percentage = RateCalculation.merge(
            [replica_snapshot.cpu_percentage for replica_snapshot in replica_snapshots]
        ).average_value_rate()

        # derived metric(s)
        if total_events_processed_per_sec == 0:
            eta_secs = -1
        else:
            eta_secs = source_backlog / total_events_processed_per_sec

        return PipelineProcessorSnapshot(
            # parent snapshot fields
            status=parent_snapshot.status,
            timestamp_millis=utils.timestamp_millis(),
            processor_id=parent_snapshot.processor_id,
            processor_type=parent_snapshot.processor_type,
            num_replicas=parent_snapshot.num_replicas,
            num_cpu_per_replica=parent_snapshot.num_cpu_per_replica,
            num_concurrency_per_replica=parent_snapshot.num_concurrency_per_replica,
            # pipeline-specific snapshot fields
            source_backlog=source_backlog,
            total_events_processed_per_sec=total_events_processed_per_sec,
            eta_secs=eta_secs,
            avg_num_elements_per_batch=avg_num_elements_per_batch,
            total_pulls_per_sec=total_pulls_per_sec,
            avg_pull_percentage_per_replica=avg_pull_percentage_per_replica,
            avg_process_time_millis_per_element=avg_process_time_millis_per_element,
            avg_process_time_millis_per_batch=avg_process_time_millis_per_batch,
            avg_pull_to_ack_time_millis_per_batch=avg_pull_to_ack_time_millis_per_batch,
            avg_cpu_percentage_per_replica=avg_cpu_percentage,
        )
