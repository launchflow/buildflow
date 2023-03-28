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
from typing import List

import ray

from buildflow.api.options import StreamingOptions

_TARGET_UTILIZATION = .5
# Don't allocate more than 66% of CPU usage to the source actors.
# This saves space to ensure that the sink and processor can be scheduled.
# TODO: This really slows down our autoscaling, ideally we could just launch
# as many as possible. The main issue though is that if we do that then there
# will be no CPU left over for the processor / sink. We need someway to tie a
# process, sink, and source together so when we scale up we get enough CPU for
# all that.
_REPLICA_CPU_RATIO = .66


def max_replicas_for_cluster(source_cpu: float):
    num_cpus = ray.cluster_resources()['CPU']

    return int((num_cpus / source_cpu * _REPLICA_CPU_RATIO))


def get_recommended_num_replicas(
    *,
    current_num_replicas: int,
    backlog: float,
    events_processed_per_replica: List[int],
    non_empty_ratio_per_replica: List[float],
    time_since_last_check: float,
    source_cpus: float,
    autoscaling_options: StreamingOptions,
) -> int:
    non_empty_ratio_sum = sum(non_empty_ratio_per_replica)
    if non_empty_ratio_per_replica:
        avg_non_empty_rate = (non_empty_ratio_sum /
                              len(non_empty_ratio_per_replica))
    else:
        avg_non_empty_rate = 0
    rate = (sum(events_processed_per_replica) / time_since_last_check)
    if events_processed_per_replica:
        avg_rate = rate / len(events_processed_per_replica) * 60
    else:
        avg_rate = 0
    # TODO: this doesn't take into account newly incoming messages so it won't
    # actually burn down the backlog in one minute. Ideally we could add some
    # metric to know we need at least N replicas for the standard rate + M
    # replicas for the backlog.
    if avg_rate != 0:
        estimated_replicas = int(backlog / avg_rate)
    else:
        estimated_replicas = 0
    if estimated_replicas > current_num_replicas:
        new_num_replicas = estimated_replicas
    elif (estimated_replicas < current_num_replicas
          and current_num_replicas > 1
          and avg_non_empty_rate < _TARGET_UTILIZATION):
        # Scale down under the following conditions.
        # - Backlog is low enough we don't need any more replicas
        # - We are running more than 1 (don't scale to 0...)
        # - Over 30% of requests are empty, i.e. we're wasting requests
        new_num_replicas = math.ceil(non_empty_ratio_sum / _TARGET_UTILIZATION)
        if new_num_replicas < estimated_replicas:
            new_num_replicas = estimated_replicas
    else:
        new_num_replicas = current_num_replicas

    max_replicas = max_replicas_for_cluster(source_cpus)
    if new_num_replicas > max_replicas:
        logging.warning(
            'reached the max allowed replicas for your cluster %s. We will add'
            ' more as your cluster scales up.', max_replicas)
        # TODO: we can look at programatically scaling this to get faster
        # autoscaling.
        new_num_replicas = max_replicas

    if new_num_replicas > autoscaling_options.max_replicas:
        logging.warning('reached the max allowed replicas of %s',
                        autoscaling_options.max_replicas)
        new_num_replicas = autoscaling_options.max_replicas
    elif new_num_replicas < autoscaling_options.min_replicas:
        logging.warning('reached the minimum allowed replicas of %s',
                        autoscaling_options.min_replicas)
        new_num_replicas = autoscaling_options.min_replicas

    if new_num_replicas != current_num_replicas:
        logging.warning('resizing from %s replicas to %s replicas.',
                        current_num_replicas, new_num_replicas)

    return new_num_replicas
