"""Common metrics used across BuildFlow"""
from typing import Optional

from .metrics import CompositeRateCounterMetric


def num_events_processed(
    processor_id, job_id, run_id, status_code: Optional[str] = None
) -> CompositeRateCounterMetric:
    tags = {
        "processor_id": processor_id,
        "JobId": job_id,
        "RunId": run_id,
    }
    if status_code is not None:
        tags["StatusCode"] = status_code
    return CompositeRateCounterMetric(
        "num_events_processed",
        description="Number of events processed by the actor. Only increments.",
        default_tags=tags,
    )


def process_time_counter(
    processor_id, job_id, run_id, status_code: Optional[str] = None
) -> CompositeRateCounterMetric:
    tags = {
        "processor_id": processor_id,
        "JobId": job_id,
        "RunId": run_id,
    }
    if status_code is not None:
        tags["StatusCode"] = status_code
    return CompositeRateCounterMetric(
        "process_time",
        description="Current process time of the actor. Goes up and down.",
        default_tags=tags,
    )
