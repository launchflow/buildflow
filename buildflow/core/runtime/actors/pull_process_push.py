import asyncio
import dataclasses
import logging
import time

import ray
from ray.util.metrics import Counter, Gauge

from buildflow.api import RuntimeStatus
from buildflow.core.processor.base import Processor
from buildflow.io.providers.base import PullProvider, PushProvider

from buildflow.api.runtime import Snapshot, AsyncRuntimeAPI

# TODO: Explore the idea of letting this class autoscale the number of threads
# it runs dynamically. Related: What if every implementation of RuntimeAPI
# could autoscale itself based on some SchedulerAPI? The Runtime tree could
# pass the global state down through the Environment object, and let each node
# decide how to scale itself. Or maybe parent runtime nodes autoscale only
# their children, and leaf nodes do not autoscale.
# Motivation: We currently have no feedback loop on the async Runtime actors,
# so we essentially have to guess how many tasks to run concurrently. We could
# use a feedback loop to dynamically scale the number of tasks based on the
# current utilization of the tasks so that way we dont have to guess and under-
# utilitize, and we also dont have to guess and over-utilitize and cause
# contention / OOM (all pending pulled batches are kept in memory).


@dataclasses.dataclass
class PullProcessPushSnapshot(Snapshot):
    utilization_score: float
    process_rate: float


@ray.remote
class PullProcessPushActor(AsyncRuntimeAPI):
    def __init__(self, processor: Processor, *, log_level: str = "INFO") -> None:
        # NOTE: Ray actors run in their own process, so we need to configure
        # logging per actor / remote task.
        logging.getLogger().setLevel(log_level)

        # setup
        self.processor = processor
        self.pull_provider: PullProvider = self.processor.source().provider()
        self.push_provider: PushProvider = self.processor.sink().provider()

        # validation
        # TODO: Validate that the schemas & types are all compatible

        # initial runtime state
        self._status = RuntimeStatus.IDLE
        self._num_running_threads = 0
        self._last_snapshot_time = time.time()
        self._num_elements_processed = 0
        self._num_pull_requests = 0
        self._num_empty_pull_responses = 0
        # metrics
        job_id = ray.get_runtime_context().get_job_id()
        self.num_events_counter = Counter(
            "num_events_processed",
            description=("Number of events processed by the actor. Only increments."),
            tag_keys=(
                "processor_name",
                "JobId",
            ),
        )
        self.num_events_counter.set_default_tags(
            {"processor_name": processor.name, "JobId": job_id}
        )
        self.process_time_gauge = Gauge(
            "process_time",
            description="Current process time of the actor. Goes up and down.",
            tag_keys=(
                "actor_name",
                "JobID",
            ),
        )
        self.process_time_gauge.set_default_tags(
            {
                "processor_name": processor.name,
                "JobId": job_id,
            }
        )

    async def run(self):
        if self._status == RuntimeStatus.IDLE:
            logging.info("Starting PullProcessPushActor...")
            self._status = RuntimeStatus.RUNNING
        elif self._status == RuntimeStatus.DRAINING:
            raise RuntimeError("Cannot run a PullProcessPushActor that is draining.")

        logging.info("Starting Thread...")
        self._num_running_threads += 1

        process_fn = self.processor.process
        while self._status == RuntimeStatus.RUNNING:
            # PULL
            batch, ack_ids = await self.pull_provider.pull()
            self._num_pull_requests += 1
            if not batch:
                self._num_empty_pull_responses += 1
                await asyncio.sleep(1)
                continue
            # PROCESS
            start_time = time.time()
            batch_results = [process_fn(element) for element in batch]
            self.process_time_gauge.set(
                (time.time() - start_time) * 1000 / len(batch_results)
            )
            # PUSH
            await self.push_provider.push(batch_results)
            # ACK
            await self.pull_provider.ack(ack_ids=ack_ids)
            self.num_events_counter.inc(len(batch))
            self._num_elements_processed += len(batch)
            # DONE -> LOOP

        self._num_running_threads -= 1
        if self._num_running_threads == 0:
            self._status = RuntimeStatus.IDLE
            logging.info("PullProcessPushActor Complete.")

        logging.info("Thread Complete.")

    async def status(self):
        # TODO: Have this method count the number of active threads
        return self._status

    async def drain(self):
        logging.info("Draining PullProcessPushActor...")
        self._status = RuntimeStatus.DRAINING

        start_time = asyncio.get_running_loop().time()
        while self._status == RuntimeStatus.DRAINING:
            if asyncio.get_running_loop().time() - start_time > 30:
                logging.warning("Drain timeout exceeded.")
                return False
            await asyncio.sleep(1)
        return True

    async def snapshot(self):
        if self._num_pull_requests == 0:
            return PullProcessPushSnapshot(utilization_score=0, process_rate=0)

        non_empty_ratio = 1 - (self._num_empty_pull_responses / self._num_pull_requests)
        process_rate = self._num_elements_processed / (
            time.time() - self._last_snapshot_time
        )
        snapshot = PullProcessPushSnapshot(
            utilization_score=non_empty_ratio,
            process_rate=process_rate,
        )
        # reset the counters
        self._last_snapshot_time = time.time()
        self._num_elements_processed = 0
        self._num_pull_requests = 0
        self._num_empty_pull_responses = 0
        return snapshot
