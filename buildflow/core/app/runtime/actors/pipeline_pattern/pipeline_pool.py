import dataclasses
import logging
from typing import List

import ray
from ray.util.placement_group import (
    placement_group,
)
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

from buildflow.core import utils
from buildflow.core.app.runtime.actors.pipeline_pattern.pull_process_push import (
    PullProcessPushActor,
    PullProcessPushSnapshot,
)
from buildflow.core.app.runtime.actors.process_pool import (
    ProcessorReplicaPoolActor,
    ProcessorSnapshot,
    ReplicaReference,
)
from buildflow.core.app.runtime.metrics import RateCalculation, SimpleGaugeMetric
from buildflow.core.options.runtime_options import ProcessorOptions
from buildflow.core.processor.patterns.pipeline import PipelineProcessor


# NOTE: The parent snapshot class includes metrics that are common to all Processor types.
@dataclasses.dataclass
class PipelineProcessorSnapshot(ProcessorSnapshot):
    source_backlog: float
    total_events_processed_per_sec: float
    eta_secs: float
    total_pulls_per_sec: float
    avg_num_elements_per_batch: float
    avg_pull_percentage_per_replica: float
    avg_process_time_millis_per_element: float
    avg_process_time_millis_per_batch: float
    avg_pull_to_ack_time_millis_per_batch: float

    def as_dict(self) -> dict:
        parent_dict = super().as_dict()
        pipeline_dict = {
            "source_backlog": self.source_backlog,
            "total_events_processed_per_sec": self.total_events_processed_per_sec,
            "eta_secs": self.eta_secs,
            "total_pulls_per_sec": self.total_pulls_per_sec,
            "avg_num_elements_per_batch": self.avg_num_elements_per_batch,
            "avg_pull_percentage_per_replica": self.avg_pull_percentage_per_replica,
            "avg_process_time_millis_per_element": self.avg_process_time_millis_per_element,  # noqa: E501
            "avg_process_time_millis_per_batch": self.avg_process_time_millis_per_batch,  # noqa: E501
            "avg_pull_to_ack_time_millis_per_batch": self.avg_pull_to_ack_time_millis_per_batch,  # noqa: E501
        }
        return {**parent_dict, **pipeline_dict}


@ray.remote(num_cpus=0.1)
class PipelineProcessorReplicaPoolActor(ProcessorReplicaPoolActor):
    """
    This actor acts as a proxy reference for a group of replica Processors.
    Runtime methods are forwarded to the replicas (i.e. 'drain'). Includes
    methods for adding and removing replicas (for autoscaling).
    """

    # TODO: Add a PipelineOptions type for pipeline-specific options
    def __init__(
        self, processor: PipelineProcessor, processor_options: ProcessorOptions
    ) -> None:
        super().__init__(processor, processor_options)
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
            },
        )

    # NOTE: Providing this method is the main purpose of this class. It allows us to
    # contain any runtime logic that applies to all Processor types.
    async def create_replica(self) -> ReplicaReference:
        ray_placement_group = await placement_group(
            [
                {
                    "CPU": self.options.num_cpus,
                }
            ],
            strategy="STRICT_PACK",
        ).ready()

        replica_actor_handle = PullProcessPushActor.options(
            num_cpus=self.options.num_cpus,
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=ray_placement_group,
                placement_group_capture_child_tasks=True,
            ),
        ).remote(self.processor, log_level=self.options.log_level)

        return ReplicaReference(
            ray_actor_handle=replica_actor_handle,
            ray_placement_group=ray_placement_group,
        )

    async def snapshot(self) -> PipelineProcessorSnapshot:
        parent_snapshot: ProcessorSnapshot = await super().snapshot()
        source_backlog = await self.source.backlog()
        # Log the current backlog so ray metrics can pick it up
        self.current_backlog_gauge.set(source_backlog)

        replica_snapshots: List[PullProcessPushSnapshot] = []
        # TODO: Dont access self.replicas directly. It should be accessed via a method
        # interface
        for replica in list(self.replicas.values()):
            snapshot: PullProcessPushSnapshot = (
                await replica.ray_actor_handle.snapshot.remote()
            )
            replica_snapshots.append(snapshot)

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

        # derived metric(s)
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
        )
