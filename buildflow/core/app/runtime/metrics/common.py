"""Common metrics used across BuildFlow"""
from .metrics import CompositeRateCounterMetric


def num_events_processed(processor_id, job_id, run_id) -> CompositeRateCounterMetric:
    return CompositeRateCounterMetric(
        "num_events_processed",
        description="Number of events processed by the actor. Only increments.",
        default_tags={
            "processor_id": processor_id,
            "JobId": job_id,
            "RunId": run_id,
        },
    )


def process_time_counter(processor_id, job_id, run_id) -> CompositeRateCounterMetric:
    return CompositeRateCounterMetric(
        "process_time",
        description="Current process time of the actor. Goes up and down.",
        default_tags={
            "processor_id": processor_id,
            "JobId": job_id,
            "RunId": run_id,
        },
    )
