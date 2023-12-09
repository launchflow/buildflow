import asyncio
import dataclasses
import logging
from typing import Any, Dict, Type

import ray
from ray import serve

from buildflow.core import utils
from buildflow.core.app.runtime._runtime import RunID, Runtime, RuntimeStatus, Snapshot
from buildflow.core.app.runtime.fastapi import create_app
from buildflow.core.options.runtime_options import ProcessorOptions
from buildflow.core.processor.patterns.collector import CollectorGroup

_MAX_SERVE_START_TRIES = 10


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
class ReceiveProcessPushSnapshot(Snapshot):
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
class ReceiveProcessPushAck(Runtime):
    def __init__(
        self,
        run_id: RunID,
        processor_group: CollectorGroup,
        *,
        processor_options: ProcessorOptions,
        flow_dependencies: Dict[Type, Any],
        serve_host: str,
        serve_port: int,
        log_level: str = "INFO",
    ) -> None:
        # NOTE: Ray actors run in their own process, so we need to configure
        # logging per actor / remote task.
        logging.getLogger().setLevel(log_level)

        # Set up actor variables
        self.run_id = run_id
        self.processor_group = processor_group
        self._status = RuntimeStatus.PENDING
        self.collector_deployment = None
        self.serve_handle = None
        self.processor_options = processor_options
        self.processors_map = {}
        self.flow_dependencies = flow_dependencies
        self.serve_host = serve_host
        self.serve_port = serve_port

    async def run(self) -> bool:
        async def process_fn(processor, *args, **kwargs):
            output = await processor.process(*args, **kwargs)
            if output is None:
                # Exclude none results
                return {"success": True}
            sink = processor.sink()
            if isinstance(output, (list, tuple)):
                if not output:
                    return {"success": True}
                push_converter = sink.push_converter(type(output[0]))
                to_send = [push_converter(result) for result in output]
            else:
                push_converter = sink.push_converter(type(output))
                to_send = [push_converter(output)]
            await sink.push(to_send)
            return {"success": True}

        app = create_app(
            processor_group=self.processor_group,
            flow_dependencies=self.flow_dependencies,
            run_id=self.run_id,
            process_fn=process_fn,
            include_output_type=False,
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
            max_concurrent_queries=self.processor_options.autoscaler_options.max_concurrent_queries,  # noqa: E501
        )
        @serve.ingress(app)
        class FastAPIWrapper:
            pass

        self.collector_deployment = FastAPIWrapper
        self.collector_application = FastAPIWrapper.bind()
        tries = 0
        while tries < _MAX_SERVE_START_TRIES:
            try:
                tries += 1
                self.serve_handle = serve.run(
                    self.collector_application,
                    host=self.serve_host,
                    port=self.serve_port,
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
        self._status = RuntimeStatus.DRAINED
        return True

    async def status(self) -> RuntimeStatus:
        return self._status

    async def snapshot(self) -> Snapshot:
        processor_snapshots = {}
        for processor in self.processor_group.processors:
            processor_snapshots[processor.processor_id] = IndividualProcessorMetrics(
                events_processed_per_sec=0,
                avg_process_time_millis=0,
            )
        if self.collector_deployment is not None:
            num_replicas = self.collector_deployment.num_replicas
        else:
            num_replicas = 0

        return ReceiveProcessPushSnapshot(
            status=self._status,
            timestamp_millis=utils.timestamp_millis(),
            processor_snapshots=processor_snapshots,
            num_replicas=num_replicas,
        )
