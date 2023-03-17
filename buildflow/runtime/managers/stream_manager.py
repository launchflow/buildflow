import asyncio
import logging
import time

import ray

from buildflow.api.options import StreamingOptions
from buildflow.runtime.managers import processors
from buildflow.runtime.ray_io import empty_io

# TODO: tune this value
_AUTO_SCALE_CHECK = 30
# If the average utilizations for actors is > .75 we will scale up the actors.
_SCALE_UP_THRESHOLD = .75
# If the average utilization for the actors is < .30 we will scale down the
# actors.
_SCALE_DOWN_THRESHOLD = .30


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
        start_replics = self.options.min_replicas
        if self.options.num_replicas:
            start_replics = self.options.num_replicas
        if start_replics <= 0:
            raise ValueError('min_replicas and num_replicas must be > 0')
        replicas = [self._add_replica() for _ in range(start_replics)]
        last_autoscale_check = time.time()
        while self.running:
            # Add a brief wait here to ensure we can check for shutdown events.
            # and free up the event loop
            await asyncio.sleep(15)
            if not self.options.autoscaling:
                # no autoscaling so just let the replicas run.
                continue
            num_replicas = len(replicas)
            now = time.time()
            if now - last_autoscale_check > _AUTO_SCALE_CHECK:
                backlog = self.processor_ref.source.backlog()
                if backlog is None:
                    continue

                last_autoscale_check = now
                rate_sum = 0
                for replica in replicas:
                    actor, _ = replica
                    rate = await actor.events_per_second.remote()
                    # Divide by 120 to get the rate over the last two minutes.
                    # This essentially allows us to compute how many replicas
                    # it would take to burn down the backlog in two minutes.
                    rate_sum += rate * 120

                avg_rate = rate_sum / num_replicas
                if avg_rate != 0:
                    estimated_replicas = backlog / avg_rate
                else:
                    estimated_replicas = 0
                print('DO NOT SUBMIT: backlog: ', backlog)
                print('DO NOT SUBMIT: avg rate: ', avg_rate)
                print('DO NOT SUBMIT: estimate_replicas: ', estimated_replicas)

                if estimated_replicas > num_replicas:
                    replicas_to_add = min(
                        estimated_replicas,
                        self.options.max_replicas) - num_replicas
                    if replicas_to_add == 0:
                        logging.warning(
                            'reached the max allowed replicas of %s',
                            num_replicas)
                    else:
                        logging.warning(
                            'resizing from %s replicas to %s replicas.',
                            num_replicas, num_replicas + replicas_to_add)
                        for _ in range(replicas_to_add):
                            replicas.append(self._add_replica())
                else:
                    print('No replicas to add, check if we should scale down.')

                # logging.warning('average utilization of %s', avg_util)
                # if avg_util > _SCALE_UP_THRESHOLD:
                #     num_replicas = min(
                #         len(replicas) * 2, self.options.max_replicas)
                #     replicas_to_add = num_replicas - len(replicas)
                #     if replicas_to_add == 0:
                #         logging.warning(
                #             'reached the max allowed replicas of %s',
                #             num_replicas)
                #     else:
                #         logging.warning('resizing to %s replicas',
                #                         num_replicas)
                #         for _ in range(replicas_to_add):
                #             replicas.append(self._add_replica())
                # elif avg_util < _SCALE_DOWN_THRESHOLD:
                #     if len(replicas) <= self.options.min_replicas:
                #         # already at the minimum so just keep.
                #         continue
                #     replicas_to_remove = math.floor(.25 * len(replicas))
                #     logging.warning('resizing to %s replicas',
                #                     len(replicas) - replicas_to_remove)
                #     tasks_to_finish = []
                #     for _ in range(replicas_to_remove):
                #         actor, tasks = replicas.pop()
                #         await actor.shutdown.remote()
                #         tasks_to_finish.extend(tasks)
                #     await asyncio.gather(*tasks_to_finish)

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
