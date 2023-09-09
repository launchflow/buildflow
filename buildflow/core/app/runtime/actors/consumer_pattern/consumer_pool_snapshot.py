import dataclasses
from typing import Dict

from buildflow.core.app.runtime.actors.process_pool import (
    ProcessorGroupSnapshot,
    IndividualProcessorSnapshot,
)
from buildflow.core.processor.processor import ProcessorID, ProcessorType


@dataclasses.dataclass
class ConsumerProcessorSnapshot(IndividualProcessorSnapshot):
    processor_id: ProcessorID
    processor_type: ProcessorType
    source_backlog: float
    total_events_processed_per_sec: float
    eta_secs: float
    total_pulls_per_sec: float
    avg_num_elements_per_batch: float
    avg_pull_percentage_per_replica: float
    avg_process_time_millis_per_element: float
    avg_process_time_millis_per_batch: float
    avg_pull_to_ack_time_millis_per_batch: float
    avg_cpu_percentage_per_replica: float

    def as_dict(self) -> dict:
        return {
            "processor_id": self.processor_id,
            "processor_type": self.processor_type.name,
            "source_backlog": self.source_backlog,
            "total_events_processed_per_sec": self.total_events_processed_per_sec,
            "eta_secs": self.eta_secs,
            "total_pulls_per_sec": self.total_pulls_per_sec,
            "avg_num_elements_per_batch": self.avg_num_elements_per_batch,
            "avg_pull_percentage_per_replica": self.avg_pull_percentage_per_replica,
            "avg_process_time_millis_per_element": self.avg_process_time_millis_per_element,  # noqa: E501
            "avg_process_time_millis_per_batch": self.avg_process_time_millis_per_batch,  # noqa: E501
            "avg_pull_to_ack_time_millis_per_batch": self.avg_pull_to_ack_time_millis_per_batch,  # noqa: E501
            "avg_cpu_percentage_per_replica": self.avg_cpu_percentage_per_replica,
        }


@dataclasses.dataclass
class ConsumerProcessorGroupSnapshot(ProcessorGroupSnapshot):
    processor_snapshots: Dict[str, ConsumerProcessorSnapshot]

    def as_dict(self) -> dict:
        parent_dict = super().as_dict()
        consumer_dict = {
            "processor_snapshots": {
                processor_id: processor_snapshot.as_dict()
                for processor_id, processor_snapshot in self.processor_snapshots.items()
            }
        }
        return {**parent_dict, **consumer_dict}
