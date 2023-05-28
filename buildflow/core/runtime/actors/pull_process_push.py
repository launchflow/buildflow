import asyncio
import signal
from concurrent.futures import Future
from typing import List

import ray
from ray.util.metrics import Counter

from buildflow.api import RuntimeAPI, RuntimeSnapshot, RuntimeStatus
from buildflow.core.processor import Processor


# TODO: Explore the idea of letting this class autoscale the number of threads
# it runs dynamically. Related: What if every implementation of RuntimeAPI
# could autoscale itself based on some SchedulerAPI? The Runtime tree could
# pass the global state down through the Environment object, and let each node
# decide how to scale itself. Or maybe parent runtime nodes autoscale only
# their children, and leaf nodes do not autoscale.
@ray.remote
class PullProcessPushActor(RuntimeAPI):

    def __init__(
        self,
        processor: Processor,
        *,
        num_threads=1,
    ) -> None:
        # configuration
        self.num_threads = num_threads
        # setup
        self.pull_source = processor.source().provider()
        self.process_fn = processor.process
        self.push_sink = processor.sink().provider()
        # initial runtime state
        self._status = RuntimeStatus.IDLE
        self._tasks: List[Future] = []
        # metrics
        self.num_events_counter = Counter(
            "num_events_processed",
            description=(
                "Number of events processed by the actor. Only increments."),
            tag_keys=("processor_name"),
        )
        self.num_events_counter.set_default_tags(
            {"processor_name": processor.name})

    def run(self):
        if self._status != RuntimeStatus.IDLE:
            raise RuntimeError('Can only start an Idle Runtime.')
        self._status = RuntimeStatus.RUNNING
        # Do we need this here? Should every Runtime have it?
        signal.signal(signal.SIGTERM, self.drain)
        signal.signal(signal.SIGINT, self.drain)
        self._tasks: List[Future] = [
            self._run.remote().future() for _ in range(self.num_threads)
        ]

    def drain(self, block: bool):
        self._status = RuntimeStatus.DRAINING
        # NOTE: setting the status to DRAINING will cause the _run method to
        # end the task loop
        if block:
            asyncio.run(asyncio.gather(*self._tasks))
        return True

    def status(self):
        if self._status == RuntimeStatus.DRAINING:
            for task in self._tasks:
                task: Future
                if not task.done():
                    return RuntimeStatus.DRAINING
            self._status = RuntimeStatus.IDLE
        return self._status

    def snapshot(self):
        return RuntimeSnapshot()

    async def _run(self):
        while self._status == RuntimeStatus.RUNNING:
            batch = await self.pull_source.pull_batch()
            batch_results = [self.process_fn(element) for element in batch]
            await self.push_sink.push_batch(batch_results)
