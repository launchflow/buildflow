import dataclasses

import ray

from buildflow.core import utils
from buildflow.core.app.runtime._runtime import RunID
from buildflow.core.app.runtime.actors.endpoint_pattern.receive_process_respond import (  # noqa: E501
    ReceiveProcessRespond,
)
from buildflow.core.app.runtime.actors.process_pool import (
    ProcessorReplicaPoolActor,
    ProcessorSnapshot,
    ReplicaReference,
)
from buildflow.core.options.runtime_options import ProcessorOptions
from buildflow.core.processor.patterns.endpoint import EndpointProcessor


@dataclasses.dataclass
class EndpointProcessorSnapshot(ProcessorSnapshot):
    total_events_processed_per_sec: float
    avg_process_time_millis_per_element: float

    def as_dict(self) -> dict:
        parent_dict = super().as_dict()
        pipeline_dict = {
            "total_events_processed_per_sec": self.total_events_processed_per_sec,
            "avg_process_time_millis_per_element": self.avg_process_time_millis_per_element,  # noqa: E501
        }
        return {**parent_dict, **pipeline_dict}


@ray.remote(num_cpus=0.1)
class EndpointProcessorPoolActor(ProcessorReplicaPoolActor):
    def __init__(
        self,
        run_id: RunID,
        processor: EndpointProcessor,
        processor_options: ProcessorOptions,
    ) -> None:
        super().__init__(run_id, processor, processor_options)
        # We only want one replica in the pool, we will start the ray serve
        # deployment with the number of replicas we want.
        self.initial_replicas = 1
        self.processor = processor
        self.replica_actor_handle = None

    async def scale(self):
        # Endpoint processors are automatically scaled by ray server.
        return

    async def create_replica(self):
        replica_id = "1"
        replica_actor_handle = ReceiveProcessRespond.remote(
            self.run_id,
            self.processor,
            processor_options=self.options,
            log_level=self.options.log_level,
        )

        return ReplicaReference(
            replica_id=replica_id,
            ray_actor_handle=replica_actor_handle,
        )

    async def snapshot(self) -> ProcessorSnapshot:
        parent_snapshot: ProcessorSnapshot = await super().snapshot()
        # TODO: we should really check that there is one and only one
        if len(self.replicas) > 0:
            replica_snapshot = await self.replicas[0].ray_actor_handle.snapshot.remote()
            num_replicas = replica_snapshot.num_replicas
            total_events_processed_per_sec = replica_snapshot.events_processed_per_sec
            avg_process_time_millis_per_element = (
                replica_snapshot.avg_process_time_millis
            )
        else:
            num_replicas = 0
            total_events_processed_per_sec = 0
            avg_process_time_millis_per_element = 0
        return EndpointProcessorSnapshot(
            status=parent_snapshot.status,
            timestamp_millis=utils.timestamp_millis(),
            processor_id=parent_snapshot.processor_id,
            processor_type=parent_snapshot.processor_type,
            num_replicas=num_replicas,
            total_events_processed_per_sec=total_events_processed_per_sec,
            avg_process_time_millis_per_element=avg_process_time_millis_per_element,
            num_concurrency_per_replica=parent_snapshot.num_concurrency_per_replica,
            num_cpu_per_replica=parent_snapshot.num_cpu_per_replica,
        )
