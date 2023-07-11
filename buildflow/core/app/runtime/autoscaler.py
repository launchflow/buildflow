"""Auto-scaler used by the stream manager.

When we do scale up?
    We check the backlog of the current source, and compare it to the
    throughput since the last autoscale event we request the number of replicas
    required to burn down the entire backlog in 60 seconds.

When do we scale down?
    First we check that we don't need to scale up. If we don't need to scale
    up, we check what the current utilization of our replicas is above 50%.
    The utilization is determined by the number of non-empty requests for data
    were made.
"""

import logging
import math

import ray
from ray.autoscaler.sdk import request_resources

from buildflow.core.app.runtime.actors.pipeline_pattern.pipeline_pool import (
    PipelineProcessorSnapshot,
)
from buildflow.core.app.runtime.actors.process_pool import ProcessorSnapshot
from buildflow.core.options.runtime_options import AutoscalerOptions
from buildflow.core.processor.processor import ProcessorType

# TODO: Make this configurable
_TARGET_UTILIZATION = 0.5


def _available_replicas(cpu_per_replica: float):
    num_cpus = ray.available_resources()["CPU"]

    return int(num_cpus / cpu_per_replica)


def _calculate_target_num_replicas_for_pipeline(
    snapshot: PipelineProcessorSnapshot, config: AutoscalerOptions
):
    cpus_per_replica = snapshot.num_cpu_per_replica
    avg_utilization_score = snapshot.avg_pull_percentage_per_replica
    total_utilization_score = avg_utilization_score * snapshot.num_replicas
    # The code below is from the previous version of the autoscaler.
    # Could probably use another pass through; might be able to simplify
    # things with the new runtime setup
    if snapshot.source_backlog is not None and snapshot.avg_num_elements_per_batch != 0:
        estimated_replicas = int(
            snapshot.source_backlog / snapshot.avg_num_elements_per_batch
        )
    else:
        estimated_replicas = 0
    if estimated_replicas > snapshot.num_replicas:
        new_num_replicas = estimated_replicas
    elif (
        estimated_replicas < snapshot.num_replicas
        and snapshot.num_replicas > 1
        and avg_utilization_score < _TARGET_UTILIZATION
    ):
        # Scale down under the following conditions.
        # - Backlog is low enough we don't need any more replicas
        # - We are running more than 1 (don't scale to 0...)
        # - Over 30% of requests are empty, i.e. we're wasting requests
        new_num_replicas = math.ceil(total_utilization_score / _TARGET_UTILIZATION)
        if new_num_replicas < estimated_replicas:
            new_num_replicas = estimated_replicas
    else:
        new_num_replicas = snapshot.num_replicas

    available_replicas = _available_replicas(cpus_per_replica)
    # If we're trying to scale to more than max replicas and max replicas
    # for our cluster is less than our total max replicas
    if new_num_replicas > config.max_replicas:
        new_num_replicas = config.max_replicas
    elif new_num_replicas < config.min_replicas:
        new_num_replicas = config.min_replicas

    if new_num_replicas > snapshot.num_replicas:
        replicas_adding = new_num_replicas - snapshot.num_replicas
        if replicas_adding > available_replicas:
            new_num_replicas = snapshot.num_replicas + available_replicas
            # Cap how much we request to ensure we're not requesting a huge amount
            cpu_to_request = new_num_replicas * cpus_per_replica * 2
            request_resources(num_cpus=math.ceil(cpu_to_request))
    else:
        # we're scaling down so only request resources that are needed for
        # the smaller amount.
        # This will override the case where we requested a bunch of
        # resources for a replicas that haven't been fufilled yet.
        request_resources(num_cpus=math.ceil(new_num_replicas * cpus_per_replica))

    if new_num_replicas != snapshot.num_replicas:
        logging.warning(
            "resizing from %s replicas to %s replicas",
            snapshot.num_replicas,
            new_num_replicas,
        )

    logging.debug(
        "---------------------------------------------------------\n"
        f"AUTOSCALER: {snapshot.num_replicas} -> {new_num_replicas}\n"
        f"AVG Utilization: {avg_utilization_score}\n"
        f"Total Utilization: {total_utilization_score}\n"
        f"AVG Process Rate: {snapshot.avg_num_elements_per_batch}\n"
        f"Backlog: {snapshot.source_backlog}\n"
        f"Estimated Replicas: {estimated_replicas}\n"
        f"Max Cluster Replicas: {available_replicas}\n"
        f"Config Max Replicas: {config.max_replicas}\n"
        f"Config Min Replicas: {config.min_replicas}\n"
        f"CPUs Per Replicas: {cpus_per_replica}\n"
        "---------------------------------------------------------\n"
    )
    return new_num_replicas


# TODO: Explore making the entire runtime autoscale
# to maximize resource utilization, we can sample the buffer size of each task
# and scale up/down based on that. We can target to use 80% of the available
# resources in the worst case scenario (99.7% of samples contained by 80% of resources).


def calculate_target_num_replicas(
    snapshot: ProcessorSnapshot, config: AutoscalerOptions
):
    if snapshot.processor_type == ProcessorType.PIPELINE:
        return _calculate_target_num_replicas_for_pipeline(snapshot, config)
    elif snapshot.processor_type == ProcessorType.COLLECTOR:
        raise NotImplementedError("Collector autoscaling not implemented yet")
    elif snapshot.processor_type == ProcessorType.CONNECTION:
        raise NotImplementedError("Connection autoscaling not implemented yet")
    elif snapshot.processor_type == ProcessorType.SERVICE:
        raise NotImplementedError("Service autoscaling not implemented yet")
    else:
        raise ValueError(f"Unknown processor type: {snapshot.processor_type}")
