import asyncio
import dataclasses
import logging
import time

import fastapi
import ray
from ray import serve

from buildflow.core import utils
from buildflow.core.app.runtime._runtime import RunID, Runtime, RuntimeStatus, Snapshot
from buildflow.core.app.runtime.metrics import (
    num_events_processed,
    process_time_counter,
)
from buildflow.core.options.runtime_options import ProcessorOptions
from buildflow.core.processor.patterns.endpoint import EndpointProcessor
from buildflow.core.processor.utils import process_types
from buildflow.io.endpoint import Method

_MAX_SERVE_START_TRIES = 10


@dataclasses.dataclass
class ReceiveProcessRespondSnapshot(Snapshot):
    status: RuntimeStatus
    timestamp_millis: int
    events_processed_per_sec: int
    avg_process_time_millis: float
    num_replicas: int

    def as_dict(self) -> dict:
        return {
            "status": self.status.name,
            "timestamp_millis": self.timestamp_millis,
            "events_processed_per_sec": self.events_processed_per_sec,  # noqa: E501
            "process_time_millis": self.avg_process_time_millis,
            "num_replicas": self.num_replicas,
        }


@ray.remote
class ReceiveProcessRespond(Runtime):
    def __init__(
        self,
        run_id: RunID,
        processor: EndpointProcessor,
        *,
        processor_options: ProcessorOptions,
        log_level: str = "INFO",
    ) -> None:
        # NOTE: Ray actors run in their own process, so we need to configure
        # logging per actor / remote task.
        logging.getLogger().setLevel(log_level)

        # Set up actor variables
        self.run_id = run_id
        self.processor = processor
        self.endpoint = self.processor.endpoint()
        self._status = RuntimeStatus.IDLE
        self.endpoint_deployment = None
        self.serve_handle = None
        self.processor_options = processor_options

    async def run(self) -> bool:
        # Setup FastAPI and Ray serve endpoint
        input_type, output_type = process_types(self.processor)
        app = fastapi.FastAPI()
        fastapi_method = None
        if self.processor.endpoint().method == Method.GET:
            fastapi_method = app.get
        elif self.processor.endpoint().method == Method.POST:
            fastapi_method = app.post
        else:
            raise NotImplementedError(
                f"Method {self.processor.endpoint().method} is not supported "
                "for endpoints."
            )

        @serve.deployment(
            route_prefix=self.endpoint.route,
            ray_actor_options={"num_cpus": self.processor_options.num_cpus},
            autoscaling_config={
                "min_replicas": self.processor_options.autoscaler_options.min_replicas,
                "initial_replicas": self.processor_options.autoscaler_options.num_replicas,  # noqa: E501
                "max_replicas": self.processor_options.autoscaler_options.max_replicas,
                "target_num_ongoing_requests_per_replica": self.processor_options.autoscaler_options.target_num_ongoing_requests_per_replica,  # noqa: E501
            },
        )
        @serve.ingress(app)
        class FastAPIWrapper:
            def __init__(self, processor, run_id):
                job_id = ray.get_runtime_context().get_job_id()
                self.processor = processor
                self.num_events_processed_counter = num_events_processed(
                    processor_id=self.processor.processor_id,
                    job_id=job_id,
                    run_id=run_id,
                )
                self.process_time_counter = process_time_counter(
                    processor_id=self.processor.processor_id,
                    job_id=job_id,
                    run_id=run_id,
                )
                self.processor.setup()

            @fastapi_method("/")
            async def root(self, request: input_type) -> output_type:
                self.num_events_processed_counter.inc()
                start_time = time.monotonic()
                output = await self.processor.process(request)
                self.process_time_counter.inc((time.monotonic() - start_time) * 1000)
                return output

            def num_events_processed(self):
                return (
                    self.num_events_processed_counter.calculate_rate().total_value_rate()
                )

            def process_time_millis(self):
                return self.process_time_counter.calculate_rate().average_value_rate()

        self.endpoint_deployment = FastAPIWrapper
        application = FastAPIWrapper.bind(self.processor, self.run_id)
        tries = 0
        while tries < _MAX_SERVE_START_TRIES:
            try:
                tries += 1
                self.serve_handle = serve.run(
                    application,
                    host="0.0.0.0",
                    port=8000,
                    name=self.processor.processor_id,
                )
                break
            except ValueError:
                # There's a edge case when we start multiple deployments from
                # different processors at the same time
                logging.exception("error starting serve, retrying in 1s")
                await asyncio.sleep(1)
        self._status = RuntimeStatus.RUNNING
        while self._status == RuntimeStatus.RUNNING:
            await asyncio.sleep(1)
        return True

    async def drain(self) -> bool:
        self._status = RuntimeStatus.DRAINING
        serve.delete(self.processor.processor_id)
        return True

    async def status(self) -> RuntimeStatus:
        return self._status

    async def snapshot(self) -> Snapshot:
        if self.serve_handle is not None:
            num_events_processed = await self.serve_handle.num_events_processed.remote()
            process_time_millis = await self.serve_handle.process_time_millis.remote()
        else:
            num_events_processed = 0
            process_time_millis = 0

        if self.endpoint_deployment is not None:
            num_replicas = self.endpoint_deployment.num_replicas
        else:
            num_replicas = 0

        return ReceiveProcessRespondSnapshot(
            status=self._status,
            timestamp_millis=utils.timestamp_millis(),
            events_processed_per_sec=num_events_processed,
            avg_process_time_millis=process_time_millis,
            num_replicas=num_replicas,
        )
