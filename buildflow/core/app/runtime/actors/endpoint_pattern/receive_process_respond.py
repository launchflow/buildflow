import asyncio
import dataclasses
import logging
import time
from typing import Any, Dict, Type

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
from buildflow.core.processor.processor import ProcessorGroup
from buildflow.core.processor.utils import process_types
from buildflow.dependencies.base import Scope

_MAX_SERVE_START_TRIES = 10


WRAPPER_TEMPLATE = """\
def wrapped_handler(
        self,
        {arg_name}: {arg_type},
        raw_request: fastapi.Request) -> None:
    return handle_request(self, request, raw_request)
"""


@dataclasses.dataclass
class IndividualProcessorMetrics:
    events_processed_per_sec: int
    avg_process_time_millis: float

    def as_dict(self) -> dict:
        return {
            "events_processed_per_sec": self.events_processed_per_sec,  # noqa: E501
            "process_time_millis": self.avg_process_time_millis,
        }


@dataclasses.dataclass
class ReceiveProcessRespondSnapshot(Snapshot):
    status: RuntimeStatus
    timestamp_millis: int
    num_replicas: int
    processor_snapshots: Dict[str, IndividualProcessorMetrics]

    def as_dict(self) -> dict:
        processor_snapshots = {
            pid: metrics.as_dict() for pid, metrics in self.processor_snapshots.items()
        }
        return {
            "status": self.status.name,
            "timestamp_millis": self.timestamp_millis,
            "events_processed_per_sec": self.events_processed_per_sec,  # noqa: E501
            "process_time_millis": self.avg_process_time_millis,
            "num_replicas": self.num_replicas,
            "processor_snapshots": processor_snapshots,
        }


@ray.remote
class ReceiveProcessRespond(Runtime):
    def __init__(
        self,
        run_id: RunID,
        processor_group: ProcessorGroup[EndpointProcessor],
        *,
        processor_options: ProcessorOptions,
        log_level: str = "INFO",
        flow_dependencies: Dict[Type, Any],
    ) -> None:
        # NOTE: Ray actors run in their own process, so we need to configure
        # logging per actor / remote task.
        logging.getLogger().setLevel(log_level)

        # Set up actor variables
        self.run_id = run_id
        self.processor_group = processor_group
        self._status = RuntimeStatus.IDLE
        self.endpoint_deployment = None
        self.serve_handle = None
        self.processor_options = processor_options
        self.processors_map = {}
        self.flow_dependencies = flow_dependencies

    async def run(self) -> bool:
        app = fastapi.FastAPI(
            title=self.processor_group.group_id,
            version="0.0.1",
            docs_url="/buildflow/docs",
        )

        @app.on_event("startup")
        def setup_processor_group():
            for processor in self.processor_group.processors:
                if hasattr(app.state, "processor_map"):
                    app.state.processor_map[processor.processor_id] = processor
                else:
                    app.state.processor_map = {processor.processor_id: processor}
                processor.setup()
                for dependency in processor.dependencies():
                    dependency.dependency.initialize(
                        self.flow_dependencies, [Scope.REPLICA]
                    )

        for processor in self.processor_group.processors:
            input_types, output_type = process_types(processor)

            class EndpointFastAPIWrapper:
                def __init__(self, processor_id, run_id, flow_dependencies):
                    self.job_id = ray.get_runtime_context().get_job_id()
                    self.run_id = run_id
                    self.num_events_processed_counter = num_events_processed(
                        processor_id=processor_id,
                        job_id=self.job_id,
                        run_id=run_id,
                    )
                    self.process_time_counter = process_time_counter(
                        processor_id=processor_id,
                        job_id=self.job_id,
                        run_id=run_id,
                    )
                    self.processor_id = processor_id
                    self.flow_dependencies = flow_dependencies

                # NOTE: we have to import this seperately because it gets run
                # inside of the ray actor
                from buildflow.core.processor.utils import add_input_types

                @add_input_types(input_types, output_type)
                async def handle_request(
                    self, raw_request: fastapi.Request, *args, **kwargs
                ) -> output_type:
                    processor = app.state.processor_map[self.processor_id]
                    self.num_events_processed_counter.inc(
                        tags={
                            "processor_id": processor.processor_id,
                            "JobId": self.job_id,
                            "RunId": self.run_id,
                        }
                    )
                    start_time = time.monotonic()
                    dependency_args = {}
                    for wrapper in processor.dependencies():
                        dependency_args[wrapper.arg_name] = wrapper.dependency.resolve(
                            self.flow_dependencies, raw_request
                        )  # noqa: E501
                    output = await processor.process(*args, **kwargs, **dependency_args)
                    self.process_time_counter.inc(
                        (time.monotonic() - start_time) * 1000,
                        tags={
                            "processor_id": processor.processor_id,
                            "JobId": self.job_id,
                            "RunId": self.run_id,
                        },
                    )
                    return output

            endpoint_wrapper = EndpointFastAPIWrapper(
                processor.processor_id, self.run_id, self.flow_dependencies
            )
            self.processors_map[processor.processor_id] = endpoint_wrapper

            app.add_api_route(
                processor.route_info().route,
                endpoint_wrapper.handle_request,
                methods=[processor.route_info().method.name],
                summary=processor.processor_id,
            )

        @serve.deployment(
            route_prefix=self.processor_group.base_route,
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
            pass

        self.endpoint_deployment = FastAPIWrapper
        self.endpoint_application = FastAPIWrapper.bind()

        tries = 0
        while tries < _MAX_SERVE_START_TRIES:
            try:
                tries += 1
                self.serve_handle = serve.run(
                    self.endpoint_application,
                    host="0.0.0.0",
                    port=8000,
                    name=self.processor_group.group_id,
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
        serve.delete(self.processor_group.group_id)
        return True

    async def status(self) -> RuntimeStatus:
        return self._status

    async def snapshot(self) -> Snapshot:
        processor_snapshots = {}
        # TODO: need to figure out local metrics
        for processor_id, fast_api_wrapper in self.processors_map.items():
            processor_snapshots[processor_id] = IndividualProcessorMetrics(
                events_processed_per_sec=0,
                avg_process_time_millis=0,
            )
        if self.endpoint_deployment is not None:
            num_replicas = self.endpoint_deployment.num_replicas
        else:
            num_replicas = 0

        return ReceiveProcessRespondSnapshot(
            status=self._status,
            timestamp_millis=utils.timestamp_millis(),
            processor_snapshots=processor_snapshots,
            num_replicas=num_replicas,
        )
