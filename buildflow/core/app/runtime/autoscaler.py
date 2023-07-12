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

_BACKLOG_BURN_THRESHOLD = 60
# We set this a little lower so io bound tasks don't scale down too much.
_TARGET_CPU_PERCENTAGE = 25


def _available_replicas(cpu_per_replica: float):
    num_cpus = ray.available_resources().get("CPU", 0)

    return int(num_cpus / cpu_per_replica)


def _calculate_target_num_replicas_for_pipeline_v2(
    snapshot: PipelineProcessorSnapshot, config: AutoscalerOptions
):
    """The autoscaler used by the pipeline runtime.

    First we check if we need to scale up. If we do not then we check
    if we should scale down.

    When do we scale up?
        We check what the current backlog is and how long it would take
        us to burn down the backlog. If it would take us longer than 60
        seconds to burn down the backlog with our current throughpu
        we scale up to the number of replicas that would be required.

        Example:
            current replicas: 2
            current throughput: 10 elements/sec
            backlog: 1000 elements

            In this example it would take us 100 seconds to burn down the
            entire backlog. Next we calculate what throughput would be need
            to burn down the backlong in 60 seconds
                want_throughput =
                    backlog / 60s
                    1000 / 60 = 16.6 elements/sec

            Then we calculate how many replicas we would need to achieve that
            and round that up to get our new number of replicas.
                new_num_replicas =
                    want_throughput / (current_throughput / current_replicas)
                    16.6 / (10 / 2) = ceil(3.3) = 4

    When do we scale down?
        We scale down by checking the CPU utilization of each replica and target
        an avergae of 25% utilization. We only check this if we are not scaling up.

        Example:
            avg_cpu_percentage_per_replica: 5%
            current_replicas: 3

            new_num_replicas =
                current_replicas / (25 / avg_cpu_percentage_per_replica)
                3 / (25 / 5) = ceil(0.6) = 1

    """
    cpus_per_replica = snapshot.num_cpu_per_replica
    backlog = snapshot.source_backlog
    num_replicas = snapshot.num_replicas
    throughput = snapshot.total_events_processed_per_sec
    throughput_per_replica = throughput / num_replicas
    avg_replica_cpu_percentage = snapshot.avg_cpu_percentage_per_replica
    available_replicas = _available_replicas(cpus_per_replica)

    logging.debug("-------------------------AUTOSCALER----------------------\n")
    logging.debug("config max replicas: %s", config.max_replicas)
    logging.debug("config min replicas: %s", config.min_replicas)
    logging.debug("cpus per replica: %s", cpus_per_replica)
    logging.debug("start num replicas: %s", num_replicas)
    logging.debug("backlog: %s", backlog)
    logging.debug("throughput: %s", throughput)
    logging.debug("throughput per replica: %s", throughput_per_replica)
    logging.debug("avg replica cpu percentage: %s", avg_replica_cpu_percentage)
    logging.debug("max available cluster replicas: %s", available_replicas)

    new_num_replicas = num_replicas
    if throughput > 0 and (backlog / throughput) > _BACKLOG_BURN_THRESHOLD:
        # Backlog is too high, scale up
        want_throughput = backlog / _BACKLOG_BURN_THRESHOLD
        logging.debug("want throughput: %s", want_throughput)
        new_num_replicas = math.ceil(want_throughput / throughput_per_replica)
    elif (
        avg_replica_cpu_percentage > 0
        and avg_replica_cpu_percentage < _TARGET_CPU_PERCENTAGE
    ):
        # Utilization is too low, scale down
        new_num_replicas = math.ceil(
            num_replicas / (_TARGET_CPU_PERCENTAGE / avg_replica_cpu_percentage)
        )

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

    logging.debug("scaling to: %s -> %s", num_replicas, new_num_replicas)
    return new_num_replicas


# TODO: Explore making the entire runtime autoscale
# to maximize resource utilization, we can sample the buffer size of each task
# and scale up/down based on that. We can target to use 80% of the available
# resources in the worst case scenario (99.7% of samples contained by 80% of resources).


def calculate_target_num_replicas(
    snapshot: ProcessorSnapshot, config: AutoscalerOptions
):
    if snapshot.processor_type == ProcessorType.PIPELINE:
        return _calculate_target_num_replicas_for_pipeline_v2(snapshot, config)
    elif snapshot.processor_type == ProcessorType.COLLECTOR:
        raise NotImplementedError("Collector autoscaling not implemented yet")
    elif snapshot.processor_type == ProcessorType.CONNECTION:
        raise NotImplementedError("Connection autoscaling not implemented yet")
    elif snapshot.processor_type == ProcessorType.SERVICE:
        raise NotImplementedError("Service autoscaling not implemented yet")
    else:
        raise ValueError(f"Unknown processor type: {snapshot.processor_type}")