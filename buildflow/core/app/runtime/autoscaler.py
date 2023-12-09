import dataclasses
import logging
import math
from typing import Optional

import ray
from ray.autoscaler.sdk import request_resources

from buildflow.core.app.runtime.actors.consumer_pattern.consumer_pool_snapshot import (
    ConsumerProcessorGroupSnapshot,
)
from buildflow.core.app.runtime.actors.process_pool import ProcessorGroupSnapshot
from buildflow.core.options.runtime_options import AutoscalerOptions
from buildflow.core.processor.processor import ProcessorGroupType


def _available_replicas(cpu_per_replica: float):
    num_cpus = ray.available_resources().get("CPU", 0)

    return int(num_cpus / cpu_per_replica)


@dataclasses.dataclass
class _CombinedMetrics:
    backlog: int
    throughput: float
    avg_replica_cpu_percentage: float
    pulls_per_sec: float

    @classmethod
    def from_snapshot(cls, snapshot: ConsumerProcessorGroupSnapshot):
        backlog = sum(s.source_backlog for s in snapshot.processor_snapshots.values())
        throughput = sum(
            s.total_events_processed_per_sec
            for s in snapshot.processor_snapshots.values()
        )
        avg_replica_cpu_percentage = sum(
            s.avg_cpu_percentage_per_replica
            for s in snapshot.processor_snapshots.values()
        ) / len(snapshot.processor_snapshots)
        avg_pulls_per_sec: float = round(
            sum(s.total_pulls_per_sec for s in snapshot.processor_snapshots.values())
            / len(snapshot.processor_snapshots),
            2,
        )
        return cls(
            backlog=backlog,
            throughput=throughput,
            avg_replica_cpu_percentage=avg_replica_cpu_percentage,
            pulls_per_sec=avg_pulls_per_sec,
        )


def _calculate_target_num_replicas_for_consumer_v2(
    *,
    current_snapshot: ConsumerProcessorGroupSnapshot,
    prev_snapshot: Optional[ConsumerProcessorGroupSnapshot],
    config: AutoscalerOptions,
):
    """The autoscaler used by the consumer runtime.

    First we check if we need to scale up. If we do not then we check
    if we should scale down.

    When do we scale up?
        We check what the current backlog is and how long it would take
        us to burn down the backlog. If it would take us longer than 60
        seconds to burn down the backlog with our current throughpu
        we scale up to the number of replicas that would be required.

        If the backlog could be burned down in less than 60 seconds we
        check if the backlog has increased since we last checked. If it
        we add the backlog rate to our current throughput and see how many
        replicas we would need to maintain that throughput. We round this
        number to only add a replica if it is a significant increase.

        Example1:
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

        Example2:
            current replicas: 4
            current throughput: 4_000 elements/sec
            backlog 100_000 elements
            current snapshot time = 2 minute

            In this example it would take us 25 seconds to burn down the
            backlog so the first case does not apply. Next we look at the
            previous backlog to see if it has grown.

            prevbacklog: 50_000 elements
            prev snapshot time: 1 minute

            In this example the backlog has grown by 50_000 elements in 1 minute
            so we would to increase our throughput to:
                backlog_growth = backlog - prev backlog
                time_gap = current snapshot time - prev snapshot time
                want_throughput =
                    current_throughput + backlog_growth / time_gap
                    4_000 + (100_000 - 50_000) / (2 * 60 - 1 * 60) = 4_833 elements/sec
                num_replicas =
                    int(round(want_throughput / throughput_per_replica))
                    int(round(4_833 / (4_000 / 4))) = 5





    When do we scale down?
        We scale down by checking the CPU utilization of each replica and target
        an avergae of 25% utilization. We only check this if we are not scaling up.

        Example:
            avg_cpu_percentage_per_replica: 20%
            current_replicas: 4

            new_num_replicas =
                current_replicas / (25 / avg_cpu_percentage_per_replica)
                4 / (25 / 20) = floor(3.2) = 3

    """
    cpus_per_replica = current_snapshot.num_cpu_per_replica
    num_replicas = current_snapshot.num_replicas
    current_metrics = _CombinedMetrics.from_snapshot(current_snapshot)
    backlog = current_metrics.backlog
    throughput = current_metrics.throughput
    throughput_per_replica = throughput / num_replicas
    avg_replica_cpu_percentage = current_metrics.avg_replica_cpu_percentage
    available_replicas = _available_replicas(cpus_per_replica)
    previous_metrics = None
    if prev_snapshot is not None:
        previous_metrics = _CombinedMetrics.from_snapshot(prev_snapshot)

    logging.debug("-------------------------AUTOSCALER----------------------\n")
    logging.debug("config max replicas: %s", config.max_replicas)
    logging.debug("config min replicas: %s", config.min_replicas)
    logging.debug("backlog burn threshold: %s", config.consumer_backlog_burn_threshold)
    logging.debug("cpu percent target: %s", config.consumer_cpu_percent_target)
    logging.debug("cpus per replica: %s", cpus_per_replica)
    logging.debug("start num replicas: %s", num_replicas)
    logging.debug("backlog: %s", backlog)
    logging.debug("throughput: %s", throughput)
    logging.debug("throughput per replica: %s", throughput_per_replica)
    logging.debug("avg replica cpu percentage: %s", avg_replica_cpu_percentage)
    logging.debug("max available cluster replicas: %s", available_replicas)

    new_num_replicas = num_replicas
    # Major backlog event. This happens when the consumer is way behind.
    if (
        throughput > 0
        and (backlog / throughput) > config.consumer_backlog_burn_threshold
    ):
        # Backlog is too high, scale up
        want_throughput = backlog / config.consumer_backlog_burn_threshold
        logging.debug("want throughput: %s", want_throughput)
        new_num_replicas = math.ceil(want_throughput / throughput_per_replica)
    # Minor backlog event. This happens when the consumer is just slightly behind during
    # it's normal operation. We only perform this check if our current replicas are
    # fully utlized.
    elif previous_metrics is not None and previous_metrics.backlog < backlog:
        # This if block is nested to avoid triggering a down scale.
        if avg_replica_cpu_percentage > config.consumer_cpu_percent_target:
            time_gap_ms = (
                current_snapshot.timestamp_millis - prev_snapshot.timestamp_millis
            )
            logging.debug("backlog gap ms : %s", time_gap_ms)
            backlog_growth = backlog - previous_metrics.backlog
            logging.debug("backlog growth: %s", backlog_growth)
            backlog_growth_per_sec = backlog_growth / (time_gap_ms / 1000)
            want_throughput = throughput + backlog_growth_per_sec
            logging.debug("want throughput: %s", want_throughput)
            new_num_replicas = math.ceil(want_throughput / throughput_per_replica)
    # No backlog but we're not processing any data so add one replica. This helps catch
    # the case where the backlog reporting is delayed.
    elif current_metrics.pulls_per_sec == 0:
        # If we hit this case add one replica
        logging.debug("adding one replica because we're not processing any data")
        new_num_replicas = num_replicas + 1
    # Scale down event.
    elif (
        avg_replica_cpu_percentage > 0
        and avg_replica_cpu_percentage < config.consumer_cpu_percent_target
    ):
        # Utilization is too low, scale down
        # We use floor here because we're trying to keep their utilization at
        # or above a target.
        new_num_replicas = math.floor(
            num_replicas
            / (config.consumer_cpu_percent_target / avg_replica_cpu_percentage)
        )
    # Sanity check to make sure we don't scale below 0.
    new_num_replicas = max(new_num_replicas, 1)

    # If we're trying to scale to more than max replicas and max replicas
    # for our cluster is less than our total max replicas
    if new_num_replicas > config.max_replicas:
        new_num_replicas = config.max_replicas
    elif new_num_replicas < config.min_replicas:
        new_num_replicas = config.min_replicas

    if new_num_replicas > current_snapshot.num_replicas:
        replicas_adding = new_num_replicas - current_snapshot.num_replicas
        if replicas_adding > available_replicas:
            new_num_replicas = current_snapshot.num_replicas + available_replicas
            # Cap how much we request to ensure we're not requesting a huge amount
            cpu_to_request = new_num_replicas * cpus_per_replica * 2
            request_resources(num_cpus=math.ceil(cpu_to_request))
    elif new_num_replicas <= current_snapshot.num_replicas:
        # We're scaling down so we don't need to request any resources. Set this to 0
        # to let the autoscaler know that we're not requesting any resources.
        request_resources(0)

    if new_num_replicas != current_snapshot.num_replicas:
        logging.warning(
            "resizing from %s replicas to %s replicas",
            current_snapshot.num_replicas,
            new_num_replicas,
        )

    logging.debug("scaling to: %s -> %s", num_replicas, new_num_replicas)
    return new_num_replicas


# TODO: Explore making the entire runtime autoscale
# to maximize resource utilization, we can sample the buffer size of each task
# and scale up/down based on that. We can target to use 80% of the available
# resources in the worst case scenario (99.7% of samples contained by 80% of resources).


def calculate_target_num_replicas(
    *,
    current_snapshot: ProcessorGroupSnapshot,
    prev_snapshot: Optional[ProcessorGroupSnapshot],
    config: AutoscalerOptions,
):
    if current_snapshot.group_type == ProcessorGroupType.CONSUMER:
        return _calculate_target_num_replicas_for_consumer_v2(
            current_snapshot=current_snapshot,
            prev_snapshot=prev_snapshot,
            config=config,
        )
    elif current_snapshot.group_type == ProcessorGroupType.COLLECTOR:
        raise NotImplementedError("Collector autoscaling not implemented yet")
    elif current_snapshot.group_type == ProcessorGroupType.SERVICE:
        raise NotImplementedError("Service autoscaling not implemented yet")
    else:
        raise ValueError(f"Unknown processor type: {current_snapshot.processor_type}")
