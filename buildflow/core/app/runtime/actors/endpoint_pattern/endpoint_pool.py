import dataclasses
from typing import Dict

import ray

from buildflow.core import utils
from buildflow.core.app.runtime._runtime import RunID
from buildflow.core.app.runtime.actors.endpoint_pattern.receive_process_respond import (  # noqa: E501
    ReceiveProcessRespond,
)
from buildflow.core.app.runtime.actors.process_pool import (
    ProcessorGroupReplicaPoolActor,
    ProcessorGroupSnapshot,
    ReplicaReference,
)
from buildflow.core.options.runtime_options import ProcessorOptions
from buildflow.core.processor.patterns.endpoint import EndpointProcessor
from buildflow.core.processor.processor import ProcessorGroup


@dataclasses.dataclass
class IndividualProcessorMetrics:
    total_events_processed_per_sec: int
    avg_process_time_millis_per_element: float

    def as_dict(self) -> dict:
        return {
            "total_events_processed_per_sec": self.total_events_processed_per_sec,  # noqa: E501
            "avg_process_time_millis_per_element": self.avg_process_time_millis_per_element,
        }


@dataclasses.dataclass
class EndpointProcessorSnapshot(ProcessorGroupSnapshot):
    processor_metrics: Dict[str, IndividualProcessorMetrics]

    def as_dict(self) -> dict:
        parent_dict = super().as_dict()
        processor_metrics = {
            pid: metrics.as_dict() for pid, metrics in self.processor_metrics.items()
        }
        endpoint_dict = {
            "processor_metrics": processor_metrics,
        }
        return {**parent_dict, **endpoint_dict}


@ray.remote(num_cpus=0.1)
class EndpointProcessorGroupPoolActor(ProcessorGroupReplicaPoolActor):
    def __init__(
        self,
        run_id: RunID,
        processor_group: ProcessorGroup[EndpointProcessor],
        processor_options: ProcessorOptions,
    ) -> None:
        super().__init__(run_id, processor_group, processor_options)
        # We only want one replica in the pool, we will start the ray serve
        # deployment with the number of replicas we want.
        self.initial_replicas = 1
        self.processor_group = processor_group
        self.replica_actor_handle = None

    async def scale(self):
        # Endpoint processors are automatically scaled by ray server.
        return

    async def create_replica(self):
        replica_id = "1"
        replica_actor_handle = ReceiveProcessRespond.remote(
            self.run_id,
            self.processor_group,
            processor_options=self.options,
            log_level=self.options.log_level,
        )

        return ReplicaReference(
            replica_id=replica_id,
            ray_actor_handle=replica_actor_handle,
        )

    async def snapshot(self) -> ProcessorGroupSnapshot:
        parent_snapshot: ProcessorGroupSnapshot = await super().snapshot()
        num_replicas = 0
        processor_metrics = {}
        if len(self.replicas) > 0:
            replica_snapshot = await self.replicas[0].ray_actor_handle.snapshot.remote()
            num_replicas = replica_snapshot.num_replicas
            for pid, metrics in replica_snapshot.processor_metrics.items():
                total_events_processed_per_sec = metrics.events_processed_per_sec
                avg_process_time_millis_per_element = metrics.avg_process_time_millis
                processor_metrics[pid] = IndividualProcessorMetrics(
                    total_events_processed_per_sec,
                    avg_process_time_millis_per_element,
                )
        return EndpointProcessorSnapshot(
            status=parent_snapshot.status,
            timestamp_millis=utils.timestamp_millis(),
            group_id=self.processor_group.group_id,
            group_type=self.processor_group.group_type,
            num_replicas=num_replicas,
            num_concurrency_per_replica=parent_snapshot.num_concurrency_per_replica,
            num_cpu_per_replica=parent_snapshot.num_cpu_per_replica,
            processor_metrics=processor_metrics,
        )
