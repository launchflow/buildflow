import dataclasses
from typing import Any, Dict, Type

import ray

from buildflow.core import utils
from buildflow.core.app.runtime._runtime import RunID
from buildflow.core.app.runtime.actors.collector_pattern.receive_process_push_ack import (  # noqa: E501
    ReceiveProcessPushAck,
)
from buildflow.core.app.runtime.actors.process_pool import (
    IndividualProcessorSnapshot,
    ProcessorGroupReplicaPoolActor,
    ProcessorGroupSnapshot,
    ReplicaReference,
)
from buildflow.core.options.runtime_options import ProcessorOptions
from buildflow.core.processor.patterns.collector import CollectorProcessor
from buildflow.core.processor.processor import (
    ProcessorGroup,
    ProcessorID,
    ProcessorType,
)


@dataclasses.dataclass
class CollectorProcessorMetrics(IndividualProcessorSnapshot):
    processor_id: ProcessorID
    processor_type: ProcessorType
    total_events_processed_per_sec: int
    avg_process_time_millis_per_element: float

    def as_dict(self) -> dict:
        return {
            "processor_id": self.processor_id,
            "processor_type": self.processor_type.name,
            "total_events_processed_per_sec": self.total_events_processed_per_sec,  # noqa: E501
            "avg_process_time_millis_per_element": self.avg_process_time_millis_per_element,  # noqa: E501
        }


@dataclasses.dataclass
class CollectorProcessorSnapshot(ProcessorGroupSnapshot):
    processor_snapshots: Dict[str, CollectorProcessorMetrics]

    def as_dict(self) -> dict:
        parent_dict = super().as_dict()
        processor_snapshots = {
            pid: metrics.as_dict() for pid, metrics in self.processor_snapshots.items()
        }
        collector_dict = {
            "processor_snapshots": processor_snapshots,
        }
        return {**parent_dict, **collector_dict}


@ray.remote
class CollectorProcessorPoolActor(ProcessorGroupReplicaPoolActor):
    def __init__(
        self,
        run_id: RunID,
        processor_group: ProcessorGroup[CollectorProcessor],
        processor_options: ProcessorOptions,
        flow_dependencies: Dict[Type, Any],
        serve_host: str,
        serve_port: int,
    ) -> None:
        super().__init__(run_id, processor_group, processor_options, flow_dependencies)
        # We only want one replica in the pool, we will start the ray serve
        # deployment with the number of replicas we want.
        self.initial_replicas = 1
        self.processor_group = processor_group
        self.replica_actor_handle = None
        self.serve_host = serve_host
        self.serve_port = serve_port

    async def scale(self):
        # Collector processors are automatically scaled by ray server.
        return

    async def create_replica(self):
        replica_id = "1"
        replica_actor_handle = ReceiveProcessPushAck.remote(
            self.run_id,
            self.processor_group,
            processor_options=self.options,
            log_level=self.options.log_level,
            flow_dependencies=self.flow_dependencies,
            serve_host=self.serve_host,
            serve_port=self.serve_port,
        )

        return ReplicaReference(
            replica_id=replica_id,
            ray_actor_handle=replica_actor_handle,
        )

    async def snapshot(self) -> ProcessorGroupSnapshot:
        parent_snapshot: ProcessorGroupSnapshot = await super().snapshot()
        num_replicas = 0
        processor_snapshots = {}
        if len(self.replicas) > 0:
            replica_snapshot = await self.replicas[0].ray_actor_handle.snapshot.remote()
            num_replicas = replica_snapshot.num_replicas
            for pid, metrics in replica_snapshot.processor_snapshots.items():
                total_events_processed_per_sec = metrics.events_processed_per_sec
                avg_process_time_millis_per_element = metrics.avg_process_time_millis
                processor_snapshots[pid] = CollectorProcessorMetrics(
                    pid,
                    ProcessorType.COLLECTOR,
                    total_events_processed_per_sec,
                    avg_process_time_millis_per_element,
                )
        return CollectorProcessorSnapshot(
            status=parent_snapshot.status,
            timestamp_millis=utils.timestamp_millis(),
            group_id=self.processor_group.group_id,
            group_type=self.processor_group.group_type,
            num_replicas=num_replicas,
            num_concurrency_per_replica=parent_snapshot.num_concurrency_per_replica,
            num_cpu_per_replica=parent_snapshot.num_cpu_per_replica,
            processor_snapshots=processor_snapshots,
        )
