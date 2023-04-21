"""Streaming manager that is responsible for autoscaling streaming pipelines

Once every two minutes the autoscaler chekcs to see if the number of replicas
needs to be increased or decreased.

When we do scale up?
    We check the backlog of the current source, and compare it to the
    throughput over the last 2 minutes. Then we request the number of replicas
    required to burn down the entire backlog in 2 minutes.

When do we scale down?
    First we check that we don't need to scale up. If we don't need to scale
    up we check what the current utilization of our replicas is above 30%.
    The utilization is determined by the number of non-empty requests for data
    were made.
"""
import asyncio
from dataclasses import dataclass
import logging
import signal
import time
from typing import Awaitable, Dict, Optional, Tuple

import ray
from ray.util.metrics import Counter, Gauge

from buildflow import utils
from buildflow.api.options import StreamingOptions
from buildflow.runtime.managers import auto_scaler
from buildflow.runtime.managers import processors
from buildflow.runtime.ray_io import empty_io

# Even though our backlog calculation is based on 2 minute intervals we do an
# auto scale check every 60 seconds. This helps ensure we use the ray resources
# as soon as they're available.
_REPLICA_CHECK_IN = 10


@dataclass
class _MetricsWrapper:
    num_events: int
    non_empty_response_ratio: float
    failed: bool = False


async def _wait_for_metrics(
        tasks: Dict[str, Awaitable]) -> Dict[str, Optional[_MetricsWrapper]]:

    async def mark(key: str,
                   coro: Awaitable) -> Tuple[str, Optional[_MetricsWrapper]]:
        try:
            num_events, empty_response_ratio, requests = await coro
            return key, _MetricsWrapper(
                num_events=num_events,
                non_empty_response_ratio=1 - empty_response_ratio)
        except asyncio.CancelledError:
            logging.warning('timeout for metrics, this can happen when an '
                            'actor is pending creation.')
            return key, None
        except Exception as e:
            logging.error('Actor died with following exception: %s', e)
            return key, _MetricsWrapper(0, 0, failed=True)

    done, pending = await asyncio.wait(
        [mark(key, coro) for key, coro in tasks.items()], timeout=15)

    for p in pending:
        p.cancel()
        await p
        done.add(p)

    final_result = {}
    for task in done:
        key, value = task.result()
        final_result[key] = value
    return final_result


@ray.remote
class _StreamManagerActor:

    def __init__(self, options: StreamingOptions,
                 processor_ref: processors.ProcessorRef) -> None:
        self.options = options
        self.processor_ref = processor_ref
        self.running = True
        self._requests = 0
        self._running_average = float("nan")
        self._sink_actor = None
        self._replicas = {}
        self.num_replicas_gauge = Gauge(
            "num_replicas",
            description="Current number of replicas. Goes up and down.",
            tag_keys=("actor_name", ),
        )
        self.num_replicas_gauge.set_default_tags(
            {"actor_name": self.__class__.__name__})
        self.cpu_per_replica = (
            processor_ref.sink.num_cpus() + processor_ref.source.num_cpus() +
            self.processor_ref.processor_instance.num_cpus())
        self.num_events_counter = Counter(
            "num_events_processed",
            description=(
                "Number of events processed by the actor. Goes up and down."),
            tag_keys=("actor_name", ),
        )
        self.num_events_counter.set_default_tags(
            {"actor_name": self.__class__.__name__})

    def _add_replica(self):
        key = str(self.processor_ref.sink)
        if isinstance(self.processor_ref.sink, empty_io.EmptySink):
            key = 'local'

        # TODO: could probably have a better way of picking these.
        # When we scale down we maybe not end up with the .25 ratio depending
        # on what source actors get turned down.
        # Could maybe solve this with ray placement groups:
        #   https://docs.ray.io/en/latest/ray-core/scheduling/placement-group.html
        process_actor = processors.ProcessActor.options(
            num_cpus=self.processor_ref.processor_instance.num_cpus()).remote(
                self.processor_ref.get_processor_replica())
        sink_actor = self.processor_ref.sink.actor(
            process_actor, self.processor_ref.source.is_streaming())

        replica_id = utils.uuid()
        source_actor = self.processor_ref.source.actor({key: sink_actor})
        num_threads = self.processor_ref.source.recommended_num_threads()
        source_pool_tasks = [
            source_actor.run.remote() for _ in range(num_threads)
        ]
        self._replicas[replica_id] = (source_actor, source_pool_tasks)

    async def _remove_replicas(self, replicas_to_remove: int):
        all_tasks = []
        actors_to_kill = []
        actor_shutdowns = []
        for _ in range(replicas_to_remove):
            to_pop = next(iter(self._replicas.keys()))
            actor, tasks = self._replicas.pop(to_pop)
            all_tasks.extend(tasks)
            actors_to_kill.append(actor)
            actor_shutdowns.append(actor.shutdown.remote())
        _, pending = await asyncio.wait(actor_shutdowns, timeout=15)
        for task in pending:
            # This can happen if the actor is not started yet, we will just
            # force it to with ray.kill below.
            task.cancel()
        _, pending = await asyncio.wait(all_tasks, timeout=15)
        for task in pending:
            # This can happen if the actor is not started yet, we will just
            # force it to with ray.kill below.
            task.cancel()
        for actor in actors_to_kill:
            ray.kill(actor, no_restart=True)

    async def run(self):
        # TODO: add better error handling for when an actor dies.
        start_replics = self.options.min_replicas
        if self.options.num_replicas:
            start_replics = self.options.num_replicas
        if start_replics <= 0:
            raise ValueError('min_replicas and num_replicas must be > 0')
        max_replicas = auto_scaler.max_replicas_for_cluster(
            self.cpu_per_replica)
        if start_replics > max_replicas:
            logging.warning(
                'requested more replicas than your current cluster can handle.'
                ' You can either start your cluster with more nodes or we '
                'will scale up the number of replicas as more nodes are added.'
            )
            start_replics = max_replicas
        # Report number of replicas we're starting with.
        self.num_replicas_gauge.set(start_replics)
        for _ in range(start_replics):
            self._add_replica()
        last_check_in = None
        while self.running:
            # Sleep until it's time for the next check in.
            await asyncio.sleep(_REPLICA_CHECK_IN)
            now = time.time()
            if last_check_in is None:
                last_check_in = now
                continue
            if (now - last_check_in > _REPLICA_CHECK_IN):
                backlog = self.processor_ref.source.backlog()
                if backlog is None:
                    continue
                events_processed = []
                non_empty_ratios = []
                metric_futures = {}
                for replica_id, replica in self._replicas.items():
                    actor, _ = replica
                    metric_futures[replica_id] = actor.metrics.remote()
                metrics = await _wait_for_metrics(metric_futures)
                new_replicas = {}
                for replica_id, metric in metrics.items():
                    if metric is not None and metric.failed:
                        logging.warning('removing dead replica with ID: %s',
                                        replica_id)
                        continue
                    new_replicas[replica_id] = self._replicas[replica_id]
                    if metric is None:
                        # Actor was pending creation so don't include it in our
                        # metrics calculation. We still want to keep track of
                        # it though for when it becomes ready.
                        continue
                    events_processed.append(metric.num_events)
                    non_empty_ratios.append(metric.non_empty_response_ratio)

                total_events_process = sum(events_processed)
                if total_events_process > 0:
                    self.num_events_counter.inc(total_events_process)
                self._replicas = new_replicas
                num_replicas = len(self._replicas)
                if self.options.autoscaling:
                    new_num_replicas = auto_scaler.get_recommended_num_replicas(  # noqa: E501
                        current_num_replicas=num_replicas,
                        backlog=backlog,
                        events_processed_per_replica=events_processed,
                        non_empty_ratio_per_replica=non_empty_ratios,
                        time_since_last_check=(now - last_check_in),
                        autoscaling_options=self.options,
                        cpus_per_replica=self.cpu_per_replica,
                    )
                else:
                    # Ensure we restart any dead replicas to get back to what
                    # the user requested.
                    new_num_replicas = start_replics
                # Report new number of replicas from the scaling event.
                self.num_replicas_gauge.set(new_num_replicas)
                if new_num_replicas > num_replicas:
                    replicas_to_add = new_num_replicas - num_replicas
                    for _ in range(replicas_to_add):
                        self._add_replica()
                elif new_num_replicas < num_replicas:
                    replicas_to_remove = num_replicas - new_num_replicas
                    await self._remove_replicas(replicas_to_remove)
                last_check_in = now

        await self._remove_replicas(len(self._replicas))

    def shutdown(self):
        self.running = False
        return True


class StreamProcessManager:

    def __init__(self, processor_ref: processors.ProcessorRef,
                 streaming_options: StreamingOptions) -> None:
        self._actor = _StreamManagerActor.remote(streaming_options,
                                                 processor_ref)
        self._manager_task = None

    def run(self):
        self._manager_task = self._actor.run.remote()
        signal.signal(signal.SIGTERM, self.shutdown)

    # NOTE: *args is added so this can be registered with SIGTERM
    def shutdown(self, *args):
        print('Shutting down processors...')
        ray.get(self._actor.shutdown.remote())
        ray.get(self._manager_task)
        print('...Sucessfully shut down processors.')

    def block(self):
        try:
            ray.get(self._manager_task)
        except KeyboardInterrupt:
            # We shutdown on sigint and sigterm. This allow users to easily
            # drain a job they launched manually, and also kill longer
            # running jobs.
            self.shutdown()
