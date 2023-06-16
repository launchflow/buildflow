import asyncio
import dataclasses
import inspect
import logging
import time

import ray

from buildflow import utils
from buildflow.api import RuntimeStatus
from buildflow.api.runtime import AsyncRuntimeAPI, Snapshot, SnapshotSummary
from buildflow.core.processor.base import Processor
from buildflow.core.runtime.metrics import (
    RateCalculation,
    CompositeRateCounterMetric,
)
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
class PullProcessPushSnapshotSummary(SnapshotSummary):
    status: RuntimeStatus
    timestamp_millis: int
    events_processed_per_sec: float
    pull_percentage: float
    process_time_millis: float
    process_batch_time_millis: float
    pull_to_ack_time_millis: float

    def as_dict(self) -> dict:
        return {
            "status": self.status.name,
            "timestamp": self.timestamp_millis,
            "events_processed_per_sec": self.events_processed_per_sec,
            "pull_percentage": self.pull_percentage,
            "process_time_millis": self.process_time_millis,
            "process_batch_time_millis": self.process_batch_time_millis,
            "pull_to_ack_time_millis": self.pull_to_ack_time_millis,
        }


# TODO: Explore using a UtilizationScore that each replica can update
# and then we can use that to determine how many replicas we need.
@dataclasses.dataclass
class PullProcessPushSnapshot(Snapshot):
    # metrics
    events_processed_per_sec: RateCalculation
    pull_percentage: RateCalculation
    process_time_millis: RateCalculation
    process_batch_time_millis: RateCalculation
    pull_to_ack_time_millis: RateCalculation
    # private fields
    _timestamp_millis: int = dataclasses.field(default_factory=utils.timestamp_millis)

    def get_timestamp_millis(self) -> int:
        return self._timestamp_millis

    def as_dict(self) -> dict:
        return {
            "status": self.status.name,
            "timestamp_millis": self.get_timestamp_millis(),
            "events_processed_per_sec": self.events_processed_per_sec.as_dict(),
            "pull_percentage": self.pull_percentage.as_dict(),
            "process_time_millis": self.process_time_millis.as_dict(),
            "process_batch_time_millis": self.process_batch_time_millis.as_dict(),
            "pull_to_ack_time_millis": self.pull_to_ack_time_millis.as_dict(),
        }

    def summarize(self) -> PullProcessPushSnapshotSummary:
        return PullProcessPushSnapshotSummary(
            status=self.status,
            timestamp_millis=self.get_timestamp_millis(),
            events_processed_per_sec=self.events_processed_per_sec.total_value_rate(),
            pull_percentage=self.pull_percentage.total_count_rate(),
            process_time_millis=self.process_time_millis.average_value_rate(),
            process_batch_time_millis=self.process_batch_time_millis.average_value_rate(),  # noqa: E501
            pull_to_ack_time_millis=self.pull_to_ack_time_millis.average_value_rate(),
        )


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
        self._last_snapshot_time = time.monotonic()
        # metrics
        self.max_batch_size = self.pull_provider.max_batch_size()
        job_id = ray.get_runtime_context().get_job_id()
        # test
        self.num_events_processed = CompositeRateCounterMetric(
            "num_events_processed",
            description="Number of events processed by the actor. Only increments.",
            default_tags={"processor_id": processor.processor_id, "JobId": job_id},
        )

        self._pull_percentage_counter = CompositeRateCounterMetric(
            "pull_percentage",
            description="Percentage of the batch size that was pulled. Goes up and down.",  # noqa: E501
            default_tags={
                "processor_id": processor.processor_id,
                "JobId": job_id,
            },
        )

        self.process_time_counter = CompositeRateCounterMetric(
            "process_time",
            description="Current process time of the actor. Goes up and down.",
            default_tags={
                "processor_id": processor.processor_id,
                "JobId": job_id,
            },
        )

        self.batch_time_counter = CompositeRateCounterMetric(
            "batch_time",
            description="Current batch process time of the actor. Goes up and down.",
            default_tags={
                "processor_id": processor.processor_id,
                "JobId": job_id,
            },
        )
        self.total_time_counter = CompositeRateCounterMetric(
            "total_time",
            description="Current total process time of the actor. Goes up and down.",
            default_tags={
                "processor_id": processor.processor_id,
                "JobId": job_id,
            },
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
            total_start_time = time.monotonic()
            try:
                response = await self.pull_provider.pull()
            except Exception:
                logging.exception("pull failed")
                continue
            if not response.payload:
                self._pull_percentage_counter.empty_inc()
                await asyncio.sleep(1)
                continue
            # PROCESS
            process_success = True
            process_start_time = time.monotonic()
            self._pull_percentage_counter.inc(
                len(response.payload) / self.max_batch_size
            )
            try:
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

                batch_process_time_millis = (
                    time.monotonic() - process_start_time
                ) * 1000
                self.batch_time_counter.inc(
                    (time.monotonic() - process_start_time) * 1000
                )
                element_process_time_millis = batch_process_time_millis / len(
                    batch_results
                )
                self.process_time_counter.inc(element_process_time_millis)

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
            self.num_events_processed.inc(len(response.payload))
            # DONE -> LOOP
            self.total_time_counter.inc((time.monotonic() - total_start_time) * 1000)

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
        snapshot = PullProcessPushSnapshot(
            status=self._status,
            events_processed_per_sec=self.num_events_processed.calculate_rate(),  # noqa: E501
            pull_percentage=self._pull_percentage_counter.calculate_rate(),
            process_time_millis=self.process_time_counter.calculate_rate(),
            process_batch_time_millis=self.batch_time_counter.calculate_rate(),
            pull_to_ack_time_millis=self.total_time_counter.calculate_rate(),
        )
        # reset the counters
        self._last_snapshot_time = time.monotonic()
        return snapshot
