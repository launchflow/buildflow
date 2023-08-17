import dataclasses

from buildflow.core.app.runtime.actors.process_pool import ProcessorSnapshot


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
    avg_cpu_percentage_per_replica: float

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
            "avg_cpu_percentage_per_replica": self.avg_cpu_percentage_per_replica,
        }
        return {**parent_dict, **pipeline_dict}
