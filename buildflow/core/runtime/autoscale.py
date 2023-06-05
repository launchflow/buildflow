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

from buildflow.core.runtime.actors.process_pool import ProcessorSnapshot
from buildflow.core.runtime.config import AutoscalerConfig

# TODO: Make this configurable
_TARGET_UTILIZATION = 0.5


def _available_replicas(cpu_per_replica: float):
    num_cpus = ray.available_resources()["CPU"]

    return int(num_cpus / cpu_per_replica)


def calculate_target_num_replicas(
    snapshot: ProcessorSnapshot, config: AutoscalerConfig
):
    cpus_per_replica = snapshot.actor_info.num_cpus

    current_num_replicas = len(snapshot.replicas)
    backlog = snapshot.source.backlog
    total_process_rate = sum(replica.process_rate for replica in snapshot.replicas)
    avg_process_rate = total_process_rate / current_num_replicas
    total_utilization_score = sum(
        replica.utilization_score for replica in snapshot.replicas
    )
    avg_utilization_score = total_utilization_score / current_num_replicas
    avg_utilization_score = avg_utilization_score / current_num_replicas
    # The code below is from the previous version of the autoscaler.
    # Could probably use another pass through; might be able to simplify
    # things with the new runtime setup
    if backlog is not None and avg_process_rate != 0:
        estimated_replicas = int(backlog / avg_process_rate)
    else:
        estimated_replicas = 0
    if estimated_replicas > current_num_replicas:
        new_num_replicas = estimated_replicas
    elif (
        estimated_replicas < current_num_replicas
        and current_num_replicas > 1
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
        new_num_replicas = current_num_replicas

    available_replicas = _available_replicas(cpus_per_replica)
    # If we're trying to scale to more than max replicas and max replicas
    # for our cluster is less than our total max replicas
    if new_num_replicas > config.max_replicas:
        new_num_replicas = config.max_replicas
    elif new_num_replicas < config.min_replicas:
        new_num_replicas = config.min_replicas

    if new_num_replicas > current_num_replicas:
        replicas_adding = new_num_replicas - current_num_replicas
        if replicas_adding > available_replicas:
            new_num_replicas = current_num_replicas + available_replicas
            # Cap how much we request to ensure we're not requesting a huge amount
            cpu_to_request = new_num_replicas * cpus_per_replica * 2
            request_resources(num_cpus=math.ceil(cpu_to_request))
    else:
        # we're scaling down so only request resources that are needed for
        # the smaller amount.
        # This will override the case where we requested a bunch of
        # resources for a replicas that haven't been fufilled yet.
        request_resources(num_cpus=math.ceil(new_num_replicas * cpus_per_replica))

    if new_num_replicas != current_num_replicas:
        logging.warning(
            "resizing from %s replicas to %s replicas",
            current_num_replicas,
            new_num_replicas,
        )

    logging.debug(
        "---------------------------------------------------------\n"
        f"AUTOSCALER: {current_num_replicas} -> {new_num_replicas}\n"
        f"AVG Utilization: {avg_utilization_score}\n"
        f"AVG Process Rate: {avg_process_rate}\n"
        f"TOTAL Proccess Rate {total_process_rate}\n"
        f"Backlog: {backlog}\n"
        f"Estimated Replicas: {estimated_replicas}\n"
        f"Max Cluster Replicas: {available_replicas}\n"
        f"Config Max Replicas: {config.max_replicas}\n"
        f"Config Min Replicas: {config.min_replicas}\n"
        f"CPUs Per Replicas: {cpus_per_replica}\n"
        "---------------------------------------------------------\n"
    )
    return new_num_replicas
