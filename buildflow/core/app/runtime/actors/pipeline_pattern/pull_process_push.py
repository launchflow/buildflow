import asyncio
import dataclasses
import inspect
import logging
import os
import time

import psutil
import ray

from buildflow.core import utils
from buildflow.core.app.runtime.actors.process_pool import ReplicaID
from buildflow.core.app.runtime._runtime import Runtime, RuntimeStatus, Snapshot, RunID
from buildflow.core.app.runtime.metrics import (
    CompositeRateCounterMetric,
    RateCalculation,
)
from buildflow.core.processor.patterns.pipeline import PipelineProcessor

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
    status: RuntimeStatus
    timestamp_millis: int
    events_processed_per_sec: RateCalculation
    pull_percentage: RateCalculation
    process_time_millis: RateCalculation
    process_batch_time_millis: RateCalculation
    pull_to_ack_time_millis: RateCalculation
    cpu_percentage: RateCalculation

    def as_dict(self) -> dict:
        return {
            "status": self.status.name,
            "timestamp_millis": self.timestamp_millis,
            "events_processed_per_sec": self.events_processed_per_sec.total_value_rate(),  # noqa: E501
            "pull_percentage": self.pull_percentage.total_count_rate(),
            "process_time_millis": self.process_time_millis.average_value_rate(),
            "process_batch_time_millis": self.process_batch_time_millis.average_value_rate(),  # noqa: E501
            "pull_to_ack_time_millis": self.pull_to_ack_time_millis.average_value_rate(),  # noqa: E501
            "cpu_percentage": self.cpu_percentage.average_value_rate(),
        }


@ray.remote
class PullProcessPushActor(Runtime):
    def __init__(
        self,
        run_id: RunID,
        processor: PipelineProcessor,
        *,
        replica_id: ReplicaID,
        log_level: str = "INFO",
    ) -> None:
        # NOTE: Ray actors run in their own process, so we need to configure
        # logging per actor / remote task.
        logging.getLogger().setLevel(log_level)

        # setup
        self.run_id = run_id
        self.processor = processor
        # NOTE: This is where the setup Processor lifecycle method is called.
        # TODO: Support Depends use case
        self.processor.setup()

        # validation
        # TODO: Validate that the schemas & types are all compatible

        # initial runtime state
        self._status = RuntimeStatus.IDLE
        self._num_running_threads = 0
        self._replica_id = replica_id
        self._last_snapshot_time = time.monotonic()
        # metrics
        self.max_batch_size = self.processor.source().max_batch_size()
        job_id = ray.get_runtime_context().get_job_id()
        # test
        self.num_events_processed = CompositeRateCounterMetric(
            "num_events_processed",
            description="Number of events processed by the actor. Only increments.",
            default_tags={
                "processor_id": processor.processor_id,
                "JobId": job_id,
                "RunId": self.run_id,
            },
        )

        self._pull_percentage_counter = CompositeRateCounterMetric(
            "pull_percentage",
            description="Percentage of the batch size that was pulled. Goes up and down.",  # noqa: E501
            default_tags={
                "processor_id": processor.processor_id,
                "JobId": job_id,
                "RunId": self.run_id,
            },
        )

        self.process_time_counter = CompositeRateCounterMetric(
            "process_time",
            description="Current process time of the actor. Goes up and down.",
            default_tags={
                "processor_id": processor.processor_id,
                "JobId": job_id,
                "RunId": self.run_id,
            },
        )

        self.batch_time_counter = CompositeRateCounterMetric(
            "batch_time",
            description="Current batch process time of the actor. Goes up and down.",
            default_tags={
                "processor_id": processor.processor_id,
                "JobId": job_id,
                "RunId": self.run_id,
            },
        )
        self.total_time_counter = CompositeRateCounterMetric(
            "total_time",
            description="Current total process time of the actor. Goes up and down.",
            default_tags={
                "processor_id": processor.processor_id,
                "JobId": job_id,
                "RunId": self.run_id,
            },
        )
        self.cpu_percentage = CompositeRateCounterMetric(
            "cpu_percentage",
            description="Current CPU percentage of a replica. Goes up and down.",
            default_tags={
                "processor_id": processor.processor_id,
                "JobId": job_id,
                "RunId": self.run_id,
                "ReplicaID": self._replica_id,
            },
        )

    async def run(self):
        pid = os.getpid()
        proc = psutil.Process(pid)
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
        source = self.processor.source()
        sink = self.processor.sink()
        pull_converter = source.pull_converter(input_type)
        push_converter = sink.push_converter(output_type)
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
            proc.cpu_percent()
            # PULL
            total_start_time = time.monotonic()
            try:
                response = await source.pull()
            except Exception:
                logging.exception("pull failed")
                continue
            if not response.payload:
                self._pull_percentage_counter.empty_inc()
                cpu_percent = proc.cpu_percent()
                if cpu_percent > 0.0:
                    self.cpu_percentage.inc(cpu_percent)
                else:
                    self.cpu_percentage.empty_inc()
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
                await sink.push(batch_results)
                # NOTE: we need this to ensure that we can call the snapshot method for
                # drains and autoscale checks
                # await asyncio.sleep(0.1)
            except Exception:
                logging.exception(
                    "failed to process batch, messages will not be acknowledged"
                )
                process_success = False
            finally:
                # ACK
                try:
                    await source.ack(response.ack_info, process_success)
                except Exception:
                    # This can happen if there is network failures for w/e reason
                    # we want to try and catch here so our runtime loop
                    # doesn't die.
                    logging.exception("failed to ack batch, will continue")
                    continue
            self.num_events_processed.inc(len(response.payload))
            # DONE -> LOOP
            self.total_time_counter.inc((time.monotonic() - total_start_time) * 1000)
            cpu_percent = proc.cpu_percent()
            if cpu_percent > 0.0:
                # Ray doesn't like it when we try to set a metric to 0
                self.cpu_percentage.inc(cpu_percent)
            else:
                self.cpu_percentage.empty_inc()

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
            timestamp_millis=utils.timestamp_millis(),
            events_processed_per_sec=self.num_events_processed.calculate_rate(),  # noqa: E501
            pull_percentage=self._pull_percentage_counter.calculate_rate(),
            process_time_millis=self.process_time_counter.calculate_rate(),
            process_batch_time_millis=self.batch_time_counter.calculate_rate(),
            pull_to_ack_time_millis=self.total_time_counter.calculate_rate(),
            cpu_percentage=self.cpu_percentage.calculate_rate(),
        )
        # reset the counters
        self._last_snapshot_time = time.monotonic()
        return snapshot
