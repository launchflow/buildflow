import asyncio
import dataclasses
import inspect
import logging
import time

import ray
from ray.util.metrics import Counter, Gauge

from buildflow.api import RuntimeStatus
from buildflow.api.runtime import Snapshot, AsyncRuntimeAPI
from buildflow.core.processor.base import Processor
from buildflow.io.providers.base import PullProvider, PushProvider

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
        # NOTE: This is where the setup Processor lifecycle method is called.
        # TODO: Support Depends use case
        self.processor.setup()

        # validation
        # TODO: Validate that the schemas & types are all compatible

        # initial runtime state
        self._status = RuntimeStatus.IDLE
        self._num_running_threads = 0
        self._last_snapshot_time = time.time()
        self._num_elements_processed = 0
        self._num_pull_requests = 0
        self._num_empty_pull_responses = 0
        self._num_pulls = 0
        self._pull_percentage = 0.0
        # metrics
        job_id = ray.get_runtime_context().get_job_id()
        self.num_events_counter = Counter(
            "num_events_processed",
            description=("Number of events processed by the actor. Only increments."),
            tag_keys=(
                "processor_id",
                "JobId",
            ),
        )
        self.num_events_counter.set_default_tags(
            {"processor_id": processor.processor_id, "JobId": job_id}
        )
        self.process_time_gauge = Gauge(
            "process_time",
            description="Current process time of the actor. Goes up and down.",
            tag_keys=(
                "processor_id",
                "JobId",
            ),
        )
        self.process_time_gauge.set_default_tags(
            {
                "processor_id": processor.processor_id,
                "JobId": job_id,
            }
        )
        self.batch_time_gauge = Gauge(
            "batch_time",
            description="Current batch process time of the actor. Goes up and down.",
            tag_keys=(
                "processor_id",
                "JobId",
            ),
        )
        self.batch_time_gauge.set_default_tags(
            {
                "processor_id": processor.processor_id,
                "JobId": job_id,
            }
        )
        self.total_time_gauge = Gauge(
            "total_time",
            description="Current total process time of the actor. Goes up and down.",
            tag_keys=(
                "processor_id",
                "JobId",
            ),
        )
        self.total_time_gauge.set_default_tags(
            {
                "processor_id": processor.processor_id,
                "JobId": job_id,
            }
        )

    async def run(self):
        if self._status == RuntimeStatus.IDLE:
            logging.info("Starting PullProcessPushActor...")
            self._status = RuntimeStatus.RUNNING
        elif self._status == RuntimeStatus.DRAINING:
            raise RuntimeError("Cannot run a PullProcessPushActor that is draining.")

        logging.debug("Starting Thread...")
        self._num_running_threads += 1

        raw_process_fn = self.processor.process
        full_arg_spec = inspect.getfullargspec(raw_process_fn)
        output_type = None
        input_type = None
        if "return" in full_arg_spec.annotations:
            output_type = full_arg_spec.annotations["return"]
            if (
                hasattr(output_type, "__origin__")
                and (output_type.__origin__ is list or output_type.__origin__ is tuple)
                and hasattr(output_type, "__args__")
            ):
                # We will flatten the return type if the outter most type is a tuple or
                # list.
                output_type = output_type.__args__[0]
        if (
            len(full_arg_spec.args) > 1
            and full_arg_spec.args[1] in full_arg_spec.annotations
        ):
            input_type = full_arg_spec.annotations[full_arg_spec.args[1]]
        pull_converter = self.processor.source().provider().pull_converter(input_type)
        push_converter = self.processor.sink().provider().push_converter(output_type)
        process_fn = raw_process_fn
        if not inspect.iscoroutinefunction(raw_process_fn):
            # Wrap the raw process function in an async function to make our calls below
            # easier
            async def wrapped_process_fn(x):
                return raw_process_fn(x)

            process_fn = wrapped_process_fn

        async def process_element(element):
            results = await process_fn(pull_converter(element))
            if isinstance(results, (list, tuple)):
                return [push_converter(result) for result in results]
            else:
                return push_converter(results)

        while self._status == RuntimeStatus.RUNNING:
            # PULL
            total_start_time = time.time()
            self._num_pulls += 1
            try:
                response = await self.pull_provider.pull()
            except Exception:
                logging.exception("pull failed")
                continue
            self._num_pull_requests += 1
            if not response.payload:
                self._num_empty_pull_responses += 1
                # Mainly for debugging purposes
                self._pull_percentage += 0.0
                await asyncio.sleep(1)
                continue
            # PROCESS
            process_success = True
            try:
                # TODO: Make this batch size configurable. Currently it is hardcoded
                # to 1000 for the pubsub source.
                batch_size = 1000
                process_start_time = time.time()
                self._pull_percentage += len(response.payload) / batch_size
                coros = []
                for element in response.payload:
                    coros.append(process_element(element))
                flattened_results = await asyncio.gather(*coros)
                batch_results = []
                for results in flattened_results:
                    if isinstance(results, list):
                        batch_results.extend(results)
                    else:
                        batch_results.append(results)
                self.process_time_gauge.set(
                    (time.time() - process_start_time) * 1000 / len(batch_results)
                )
                self.batch_time_gauge.set((time.time() - process_start_time) * 1000)

                # PUSH
                await self.push_provider.push(batch_results)
            except Exception:
                logging.exception(
                    "failed to process batch, messages will not be acknowledged"
                )
                process_success = False
            finally:
                # ACK
                await self.pull_provider.ack(response.ack_info, process_success)
            self.num_events_counter.inc(len(response.payload))
            self._num_elements_processed += len(response.payload)
            # DONE -> LOOP
            self.total_time_gauge.set((time.time() - total_start_time) * 1000)

        self._num_running_threads -= 1
        if self._num_running_threads == 0:
            self._status = RuntimeStatus.IDLE
            logging.info("PullProcessPushActor Complete.")

        logging.debug("Thread Complete.")

    async def status(self):
        # TODO: Have this method count the number of active threads
        return self._status

    async def drain(self):
        logging.info("Draining PullProcessPushActor...")
        self._status = RuntimeStatus.DRAINING

        while self._status == RuntimeStatus.DRAINING:
            await asyncio.sleep(1)
        return True

    async def snapshot(self):
        if self._num_pull_requests == 0:
            return PullProcessPushSnapshot(utilization_score=0, process_rate=0)

        # Previous utilitization metric option:
        # non_empty_ratio = 1 - (
        #     self._num_empty_pull_responses / self._num_pull_requests)
        # utilization_score = non_empty_ratio

        utilization_score = self._pull_percentage / self._num_pull_requests

        process_rate = self._num_elements_processed / (
            time.time() - self._last_snapshot_time
        )
        snapshot = PullProcessPushSnapshot(
            utilization_score=utilization_score,
            process_rate=process_rate,
        )
        # reset the counters
        self._last_snapshot_time = time.time()
        self._num_pull_requests = 0
        self._num_empty_pull_responses = 0
        self._pull_percentage = 0.0
        self._num_elements_processed = 0
        self._num_pull_requests = 0
        self._num_empty_pull_responses = 0
        return snapshot
