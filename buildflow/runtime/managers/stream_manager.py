import asyncio
import logging
import time

import ray

from buildflow.api.options import StreamingOptions
from buildflow.runtime.managers import processors
from buildflow.runtime.ray_io import empty_io

_AUTO_SCALE_CHECK = 30
# Don't allocate more than 66% of CPU usage to the source actors.
# This saves space to ensure that the sink and processor can be scheduled.
# TODO: This really slows down our autoscaling, ideally we could just launch
# as many as possible. The main issue though is that if we do that then there
# will be no CPU left over for the processor / sink. We need someway to tie a
# process, sink, and source together so when we scale up we get enough CPU for
# all that.
_REPLICA_CPU_RATIO = .66
_TARGET_UTILIZATION = .75


@ray.remote
class _StreamManagerActor:

    def __init__(self, options: StreamingOptions,
                 processor_ref: processors.ProcessorRef) -> None:
        self.options = options
        self.processor_ref = processor_ref
        self.running = True
        self._requests = 0
        self._running_average = float("nan")
        self._process_actor = None
        self._sink_actor = None

    def _add_replica(self):
        key = str(self.processor_ref.sink)
        if isinstance(self.processor_ref.sink, empty_io.EmptySink):
            key = 'local'
        if self._process_actor is None:
            self._processor_actor = processors.ProcessActor.remote(
                self.processor_ref.get_processor_replica())
        if self._sink_actor is None:
            self._sink_actor = self.processor_ref.sink.actor(
                self._processor_actor.process_batch.remote,
                self.processor_ref.source.is_streaming())

        source_actor = self.processor_ref.source.actor({key: self._sink_actor})
        num_threads = self.processor_ref.source.recommended_num_threads()
        source_pool_tasks = [
            source_actor.run.remote() for _ in range(num_threads)
        ]
        return source_actor, source_pool_tasks

    async def run(self):
        # TODO: add better error handling for when an actor dies.
        start_replics = self.options.min_replicas
        if self.options.num_replicas:
            start_replics = self.options.num_replicas
        if start_replics <= 0:
            raise ValueError('min_replicas and num_replicas must be > 0')
        replicas = [self._add_replica() for _ in range(start_replics)]
        last_autoscale_check = None
        while self.running:
            # Add a brief wait here to ensure we can check for shutdown events.
            # and free up the event loop
            await asyncio.sleep(15)
            if not self.options.autoscaling:
                # no autoscaling so just let the replicas run.
                continue
            num_replicas = len(replicas)
            now = time.time()
            if (last_autoscale_check is None
                    or last_autoscale_check > _AUTO_SCALE_CHECK):
                backlog = self.processor_ref.source.backlog()
                if backlog is None:
                    continue

                last_autoscale_check = now
                rate_sum = 0
                new_replics = []
                for replica in replicas:
                    actor, tasks = replica
                    try:
                        rate = await actor.events_per_second.remote()
                    except Exception as e:
                        logging.error(
                            'Actor died with following exception: %s', e)
                        continue
                    new_replics.append((actor, tasks))
                    # Multiply by 120 for the rate over the last two minutes.
                    # This essentially allows us to compute how many replicas
                    # it would take to burn down the backlog in two minutes.
                    rate_sum += rate * 120
                replicas = new_replics

                avg_rate = rate_sum / num_replicas
                if avg_rate != 0:
                    estimated_replicas = backlog / avg_rate
                else:
                    estimated_replicas = 0
                if estimated_replicas > num_replicas:
                    if num_replicas == self.options.max_replicas:
                        logging.warning(
                            'reached the max allowed replicas of %s',
                            num_replicas)
                        continue
                    new_num_replicas = min(estimated_replicas,
                                           self.options.max_replicas)
                    num_cpus = ray.cluster_resources()['CPU']

                    max_replicas = int(
                        (num_cpus / self.processor_ref.source.num_cpus() *
                         _REPLICA_CPU_RATIO))

                    new_num_replicas = min(new_num_replicas, max_replicas)
                    replicas_to_add = new_num_replicas - num_replicas
                    if replicas_to_add == 0:
                        logging.warning(
                            'reached the max allowed replicas of %s based on available cluster resources. more will be added as your cluster scales.',  # noqa: E501
                            num_replicas)
                        continue
                    else:
                        logging.warning(
                            'resizing from %s replicas to %s replicas.',
                            num_replicas, num_replicas + replicas_to_add)
                        for _ in range(replicas_to_add):
                            replicas.append(self._add_replica())
                else:
                    # Compute the average utilization over 9 seconds with 3
                    # samples.
                    cpu_usage_sum = 0
                    cluster_cpus = ray.cluster_resources()['CPU']
                    for _ in range(3):
                        cpu_usage_sum += (cluster_cpus -
                                          ray.available_resources()['CPU'])
                        await asyncio.sleep(3)
                    avg_cpu_usage = cpu_usage_sum / 3
                    # TODO fine a good value for this
                    # If our avg util is < 60 scale down the number of replicas
                    if avg_cpu_usage / cluster_cpus <= _TARGET_UTILIZATION:
                        target_cpu = avg_cpu_usage / _TARGET_UTILIZATION
                        target_num_replicas = (
                            target_cpu / self.processor_ref.source.num_cpus())
                        if target_num_replicas > num_replicas:
                            all_tasks = []
                            for _ in range(num_replicas - target_num_replicas):
                                actor, tasks = replicas.pop()
                                all_tasks.extend(tasks)
                                await actor.shutdown.remote()
                            await asyncio.gather(*all_tasks)

        all_tasks = []
        for replica in replicas:
            actor, tasks = replica
            await actor.shutdown.remote()
            all_tasks.extend(tasks)
        await asyncio.gather(*all_tasks)

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

    def _shutdown(self):
        ray.get(self._actor.shutdown.remote())

    def block(self):
        try:
            ray.get(self._manager_task)
        except KeyboardInterrupt:
            print('Shutting down processors...')
            self._shutdown()
            ray.get(self._manager_task)
            print('...Sucessfully shut down processors.')
