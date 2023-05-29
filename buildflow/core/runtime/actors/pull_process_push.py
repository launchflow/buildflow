import ray
from ray.util.metrics import Counter

from buildflow.api import RuntimeStatus
from buildflow.core.processor.base import Processor
from buildflow.io.providers.base import PullProvider, PushProvider
import asyncio

# TODO: Explore the idea of letting this class autoscale the number of threads
# it runs dynamically. Related: What if every implementation of RuntimeAPI
# could autoscale itself based on some SchedulerAPI? The Runtime tree could
# pass the global state down through the Environment object, and let each node
# decide how to scale itself. Or maybe parent runtime nodes autoscale only
# their children, and leaf nodes do not autoscale.
# TODO: add Threadable interface for awaitable run() method

# memory size is: 0.25GB
memory = 256 * 1024 * 1024


@ray.remote(num_cpus=1)
class PullProcessPushActor:

    def __init__(self, processor: Processor) -> None:
        # setup
        self.processor = processor
        self.pull_provider: PullProvider = self.processor.source().provider()
        self.push_provider: PushProvider = self.processor.sink().provider()

        # validation
        # TODO: Validate that the schemas & types are all compatible

        # initial runtime state
        self._status = RuntimeStatus.IDLE
        self._num_running_threads = 0
        # metrics
        self.num_events_counter = Counter(
            "num_events_processed",
            description=(
                "Number of events processed by the actor. Only increments."),
            tag_keys=("processor_name", ),
        )
        self.num_events_counter.set_default_tags(
            {"processor_name": processor.name})

    async def run(self):
        if self._status == RuntimeStatus.IDLE:
            print('Starting PullProcessPushActor...')
            self._status = RuntimeStatus.RUNNING
        elif self._status == RuntimeStatus.DRAINING:
            raise RuntimeError(
                'Cannot run a PullProcessPushActor that is draining.')

        print('starting Thread...')
        self._num_running_threads += 1

        process_fn = self.processor.process
        while self._status == RuntimeStatus.RUNNING:
            # print('Starting Pull')
            batch, ack_ids = await self.pull_provider.pull()
            if not batch:
                await asyncio.sleep(1)
                continue
            # print('Starting Process')
            batch_results = [process_fn(element) for element in batch]
            # print('Starting Push')
            await self.push_provider.push(batch_results)
            # print('Starting Ack')
            await self.pull_provider.ack(ack_ids=ack_ids)
            self.num_events_counter.inc(len(batch))
            # print('Ack Complete')

        self._num_running_threads -= 1
        if self._num_running_threads == 0:
            self._status = RuntimeStatus.IDLE
            print('Thread Complete')

        print('PullProcessPushActor Complete')

    async def status(self):
        # TODO: Have this method count the number of active threads
        return self._status

    async def drain(self):
        print('Draining PullProcessPushActor...')
        self._status = RuntimeStatus.DRAINING

        start_time = asyncio.get_running_loop().time()
        while self._status == RuntimeStatus.DRAINING:
            if asyncio.get_running_loop().time() - start_time > 60:
                print('Drain timeout exceeded')
                break
            await asyncio.sleep(1)
        return True
