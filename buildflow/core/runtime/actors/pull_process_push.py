import ray
from ray.util.metrics import Counter
import dataclasses

from buildflow.api import RuntimeStatus
from buildflow.core.processor.base import Processor
from buildflow.io.providers.base import PullProvider, PushProvider
import asyncio
import time

# TODO: Explore the idea of letting this class autoscale the number of threads
# it runs dynamically. Related: What if every implementation of RuntimeAPI
# could autoscale itself based on some SchedulerAPI? The Runtime tree could
# pass the global state down through the Environment object, and let each node
# decide how to scale itself. Or maybe parent runtime nodes autoscale only
# their children, and leaf nodes do not autoscale.
# TODO: add Threadable interface for awaitable run() method

# TODO: Make this configurable
NUM_CPUS = 1


@dataclasses.dataclass
class CheckinResult:
    utilization_score: float
    process_rate: float


@ray.remote(num_cpus=NUM_CPUS)
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
        self._last_checkin_time = time.time()
        self._num_elements_processed = 0
        self._num_pull_requests = 0
        self._num_empty_pull_responses = 0
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
            self._num_pull_requests += 1
            if not batch:
                self._num_empty_pull_responses += 1
                await asyncio.sleep(1)
                continue
            # print('Starting Process')
            batch_results = [process_fn(element) for element in batch]
            # print('Starting Push')
            await self.push_provider.push(batch_results)
            # print('Starting Ack')
            await self.pull_provider.ack(ack_ids=ack_ids)
            self.num_events_counter.inc(len(batch))
            self._num_elements_processed += len(batch)
            # print('Ack Complete')

        self._num_running_threads -= 1
        if self._num_running_threads == 0:
            self._status = RuntimeStatus.IDLE
            print('Thread Complete')

        print('PullProcessPushActor Complete')

    async def status(self):
        # TODO: Have this method count the number of active threads
        return self._status

    async def checkin(self):
        if self._num_pull_requests == 0:
            return CheckinResult(utilization_score=0, process_rate=0)

        non_empty_ratio = 1 - (self._num_empty_pull_responses /
                               self._num_pull_requests)
        process_rate = self._num_elements_processed / (time.time() -
                                                       self._last_checkin_time)
        checkin_result = CheckinResult(
            utilization_score=non_empty_ratio,
            process_rate=process_rate,
        )
        # reset the counters
        self._last_checkin_time = time.time()
        self._num_elements_processed = 0
        self._num_pull_requests = 0
        self._num_empty_pull_responses = 0
        return checkin_result

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
