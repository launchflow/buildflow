import asyncio
import dataclasses
import logging
import os
import time
from typing import Any, Dict, Type

import psutil
import ray

from buildflow.core import utils
from buildflow.core.app.runtime._runtime import RunID, Runtime, RuntimeStatus, Snapshot
from buildflow.core.app.runtime.actors.process_pool import ReplicaID
from buildflow.core.app.runtime.metrics import (
    CompositeRateCounterMetric,
    RateCalculation,
    num_events_processed,
    process_time_counter,
)
from buildflow.core.processor.patterns.consumer import ConsumerProcessor
from buildflow.core.processor.processor import ProcessorGroup
from buildflow.core.processor.utils import process_types
from buildflow.dependencies.base import (
    Scope,
    initialize_dependencies,
    resolve_dependencies,
)

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
class IndividualProcessorMetrics:
    events_processed_per_sec: RateCalculation
    pull_percentage: RateCalculation
    process_time_millis: RateCalculation
    process_batch_time_millis: RateCalculation
    pull_to_ack_time_millis: RateCalculation
    cpu_percentage: RateCalculation

    def as_dict(self) -> dict:
        return {
            "events_processed_per_sec": self.events_processed_per_sec.total_value_rate(),  # noqa: E501
            "pull_percentage": self.pull_percentage.total_count_rate(),
            "process_time_millis": self.process_time_millis.average_value_rate(),
            "process_batch_time_millis": self.process_batch_time_millis.average_value_rate(),  # noqa: E501
            "pull_to_ack_time_millis": self.pull_to_ack_time_millis.average_value_rate(),  # noqa: E501
            "cpu_percentage": self.cpu_percentage.average_value_rate(),
        }


@dataclasses.dataclass
class PullProcessPushSnapshot(Snapshot):
    status: RuntimeStatus
    timestamp_millis: int
    processor_snapshots: Dict[str, IndividualProcessorMetrics]

    def as_dict(self) -> dict:
        snapshot_dict = {
            "status": self.status.name,
            "timestamp_millis": self.timestamp_millis,
        }
        for processor_id, processor_snapshot in self.processor_snapshots.items():
            snapshot_dict[processor_id] = processor_snapshot.as_dict()
        return snapshot_dict


@ray.remote
class PullProcessPushActor(Runtime):
    def __init__(
        self,
        run_id: RunID,
        processor_group: ProcessorGroup[ConsumerProcessor],
        *,
        replica_id: ReplicaID,
        flow_dependencies: Dict[Type, Any],
        log_level: str = "INFO",
    ) -> None:
        # NOTE: Ray actors run in their own process, so we need to configure
        # logging per actor / remote task.
        logging.getLogger().setLevel(log_level)

        # setup
        self.run_id = run_id
        self.processor_group = processor_group
        self.flow_dependencies = flow_dependencies

        # validation
        # TODO: Validate that the schemas & types are all compatible

        # initial runtime state
        self._status = RuntimeStatus.PENDING
        self._num_running_threads = 0
        self._replica_id = replica_id
        self._last_snapshot_time = time.monotonic()
        # metrics
        job_id = ray.get_runtime_context().get_job_id()
        self.num_events_processed = {}
        self.process_time_counter = {}
        self.pull_percentage_counter = {}
        self.batch_time_counter = {}
        self.total_time_counter = {}
        self.cpu_percentage = {}
        for processor in self.processor_group.processors:
            processor_id = processor.processor_id

            self.num_events_processed[processor_id] = num_events_processed(
                processor_id=processor_id,
                job_id=job_id,
                run_id=self.run_id,
            )
            self.process_time_counter[processor_id] = process_time_counter(
                processor_id=processor_id,
                job_id=job_id,
                run_id=self.run_id,
            )

            self.pull_percentage_counter[processor_id] = CompositeRateCounterMetric(
                "pull_percentage",
                description="Percentage of the batch size that was pulled. Goes up and down.",  # noqa: E501
                default_tags={
                    "processor_id": processor_id,
                    "JobId": job_id,
                    "RunId": self.run_id,
                },
            )

            self.batch_time_counter[processor_id] = CompositeRateCounterMetric(
                "batch_time",
                description="Current batch process time of the actor. Goes up and down.",  # noqa: E501
                default_tags={
                    "processor_id": processor_id,
                    "JobId": job_id,
                    "RunId": self.run_id,
                },
            )
            self.total_time_counter[processor_id] = CompositeRateCounterMetric(
                "total_time",
                description="Current total process time of the actor. Goes up and down.",  # noqa: E501
                default_tags={
                    "processor_id": processor_id,
                    "JobId": job_id,
                    "RunId": self.run_id,
                },
            )
            self.cpu_percentage[processor_id] = CompositeRateCounterMetric(
                "cpu_percentage",
                description="Current CPU percentage of a replica. Goes up and down.",
                default_tags={
                    "processor_id": processor_id,
                    "JobId": job_id,
                    "RunId": self.run_id,
                    "ReplicaID": self._replica_id,
                },
            )

    async def initialize(self):
        for processor in self.processor_group.processors:
            processor.setup()
            await initialize_dependencies(
                processor.dependencies(), self.flow_dependencies, [Scope.REPLICA]
            )

    async def run(self):
        if self._status == RuntimeStatus.PENDING:
            logging.info("Starting PullProcessPushActor...")
            self._status = RuntimeStatus.RUNNING
        elif self._status == RuntimeStatus.DRAINING:
            logging.info("PullProcessPushActor is already draining will not start.")
            return

        logging.debug("Starting Thread...")
        self._num_running_threads += 1
        tasks = []
        for processor in self.processor_group.processors:
            tasks.append(asyncio.create_task(self._run_processor(processor)))
        await asyncio.gather(*tasks)
        self._num_running_threads -= 1
        if self._num_running_threads <= 0:
            # Only mark this as drained if all the threads have completed.
            self._status = RuntimeStatus.DRAINED
            logging.info("PullProcessPushActor Complete.")

        logging.debug("Thread Complete.")

    async def _run_processor(self, processor: ConsumerProcessor):
        processor_id = processor.processor_id
        pid = os.getpid()
        proc = psutil.Process(pid)

        input_types, output_type = process_types(processor)
        if len(input_types) != 1:
            raise ValueError("At least one input type must be specified for consumers")
        input_type = input_types[0]
        source = processor.source()
        sink = processor.sink()
        pull_converter = source.pull_converter(input_type.arg_type)
        push_converter = sink.push_converter(output_type)
        process_fn = processor.process

        async def process_element(element, *args, **kwargs):
            results = await process_fn(pull_converter(element), *args, **kwargs)
            if results is None:
                # Exclude none results
                return
            elif isinstance(results, (list, tuple)):
                return [push_converter(result) for result in results]
            else:
                return push_converter(results)

        max_batch_size = source.max_batch_size()
        while self._status == RuntimeStatus.RUNNING:
            # Add a small sleep here so none async sources can yield
            # otherwise drain signals never get received.
            # TODO: figure out away to remove this sleep
            await asyncio.sleep(0.001)
            proc.cpu_percent()
            # PULL
            total_start_time = time.monotonic()
            try:
                response = await source.pull()
            except Exception:
                logging.exception("pull failed")
                continue
            if not response.payload:
                self.pull_percentage_counter[processor_id].empty_inc()
                cpu_percent = proc.cpu_percent()
                if cpu_percent > 0.0:
                    self.cpu_percentage[processor_id].inc(cpu_percent)
                else:
                    self.cpu_percentage[processor_id].empty_inc()
                continue
            # PROCESS
            process_success = True
            process_start_time = time.monotonic()

            if max_batch_size > 0:
                self.pull_percentage_counter[processor_id].inc(
                    len(response.payload) / source.max_batch_size()
                )
            try:
                coros = []
                for element in response.payload:
                    dependency_args = dependency_args = await resolve_dependencies(
                        processor.dependencies(), self.flow_dependencies
                    )
                    coros.append(process_element(element, **dependency_args))
                flattened_results = await asyncio.gather(*coros)
                batch_results = []
                for results in flattened_results:
                    if results is None:
                        # Exclude none from the users batch
                        continue
                    if isinstance(results, list):
                        batch_results.extend(results)
                    else:
                        batch_results.append(results)

                batch_process_time_millis = (
                    time.monotonic() - process_start_time
                ) * 1000
                self.batch_time_counter[processor_id].inc(
                    (time.monotonic() - process_start_time) * 1000
                )
                element_process_time_millis = batch_process_time_millis / len(
                    response.payload
                )
                self.process_time_counter[processor_id].inc(element_process_time_millis)

                # PUSH
                if batch_results:
                    await sink.push(batch_results)
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
            self.num_events_processed[processor_id].inc(len(response.payload))
            # DONE -> LOOP
            self.total_time_counter[processor_id].inc(
                (time.monotonic() - total_start_time) * 1000
            )
            cpu_percent = proc.cpu_percent()
            if cpu_percent > 0.0:
                # Ray doesn't like it when we try to set a metric to 0
                self.cpu_percentage[processor_id].inc(cpu_percent)
            else:
                self.cpu_percentage[processor_id].empty_inc()

    async def status(self):
        # TODO: Have this method count the number of active threads
        return self._status

    async def drain(self):
        logging.info("Draining PullProcessPushActor...")
        self._status = RuntimeStatus.DRAINING

        while self._status == RuntimeStatus.DRAINING:
            await asyncio.sleep(1)
        return True

    async def num_active_threads(self):
        return self._num_running_threads

    async def snapshot(self):
        individual_metrics = {}
        for processor in self.processor_group.processors:
            processor_id = processor.processor_id
            individual_metrics[processor_id] = IndividualProcessorMetrics(
                events_processed_per_sec=self.num_events_processed[
                    processor_id
                ].calculate_rate(),
                pull_percentage=self.pull_percentage_counter[
                    processor_id
                ].calculate_rate(),
                process_time_millis=self.process_time_counter[
                    processor_id
                ].calculate_rate(),
                process_batch_time_millis=self.batch_time_counter[
                    processor_id
                ].calculate_rate(),
                pull_to_ack_time_millis=self.total_time_counter[
                    processor_id
                ].calculate_rate(),
                cpu_percentage=self.cpu_percentage[processor_id].calculate_rate(),
            )
        snapshot = PullProcessPushSnapshot(
            status=self._status,
            timestamp_millis=utils.timestamp_millis(),
            processor_snapshots=individual_metrics,
        )
        # reset the counters
        self._last_snapshot_time = time.monotonic()
        return snapshot
