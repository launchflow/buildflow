import asyncio
import copy
import dataclasses
import json
import logging
import os
import signal
import traceback
from typing import Dict, Optional


import requests

from buildflow import utils
from buildflow.api import NodeResults, ProcessorAPI, SourceType, SinkType, options
from buildflow.runtime.managers import batch_manager
from buildflow.runtime.managers import stream_manager


@dataclasses.dataclass
class _ProcessorRef:
    processor_instance: ProcessorAPI
    source: SourceType
    sink: SinkType

    def get_processor_replica(self):
        return copy.deepcopy(self.processor_instance)


_SESSION_DIR = os.path.join(os.path.expanduser("~"), ".config", "buildflow")
_SESSION_FILE = os.path.join(_SESSION_DIR, "build_flow_usage.json")


@dataclasses.dataclass
class Session:
    id: str


@dataclasses.dataclass
class _StreamingResults(NodeResults):
    def __init__(self, node_name: str) -> None:
        super().__init__(node_name)
        self._managers = []
        self._manager_tasks = []

    def add_manager(self, manager: stream_manager.StreamProcessManager):
        self._managers.append(manager)
        self._manager_tasks.append(manager.manager_task)

    async def output(self, register_shutdown: bool = True):
        if register_shutdown:
            loop = asyncio.get_event_loop()
            for signame in ("SIGINT", "SIGTERM"):
                loop.add_signal_handler(
                    getattr(signal, signame),
                    lambda: asyncio.create_task(self.shutdown()),
                )
        return await asyncio.gather(*self._manager_tasks)

    async def shutdown(self, *args):
        print(f"Shutting down node: {self.node_name}...")
        shutdown_tasks = []
        for manager in self._managers:
            # same time.
            shutdown_tasks.append(manager.shutdown())
        await asyncio.gather(*shutdown_tasks)
        await asyncio.gather(*self._manager_tasks)
        print(f"...node: {self.node_name} shut down.")


@dataclasses.dataclass
class _BatchResults(NodeResults):
    def __init__(self, node_name: str) -> None:
        super().__init__(node_name)
        self._processor_tasks = {}

    def _add_processor_task(self, processor_id: str, tasks):
        self._processor_tasks[processor_id] = tasks

    async def _get_results(self):
        final_output = {}
        for proc_id, batch_ref in self._processor_tasks.items():
            proc_output = {}
            output = await batch_ref
            for key, value in output.items():
                if key in proc_output:
                    proc_output[key].extend(value)
                else:
                    proc_output[key] = value
            final_output[proc_id] = proc_output
        return final_output

    async def output(self):
        return await self._get_results()


def _load_session():
    try:
        os.makedirs(_SESSION_DIR, exist_ok=True)
        if os.path.exists(_SESSION_FILE):
            with open(_SESSION_FILE, "r") as f:
                session_info = json.load(f)
                return Session(**session_info)
        else:
            session = Session(id=utils.uuid())
            with open(_SESSION_FILE, "w") as f:
                json.dump(dataclasses.asdict(session), f)
            return session
    except Exception as e:
        logging.debug("failed to load session id with error: %s", e)


class Runtime:
    def __init__(self):
        self._processors: Dict[str, _ProcessorRef] = {}
        self._session = _load_session()

    def run(
        self,
        *,
        streaming_options: options.StreamingOptions,
        enable_resource_creation: bool,
        disable_usage_stats: bool,
        node_name: str,
        blocking: bool = True,
    ):
        if (
            not disable_usage_stats
            and "BUILDFLOW_USAGE_STATS_DISABLE" not in os.environ
        ):
            print(
                "Usage stats collection is enabled. To disable set "
                "`disable_usage_stats` in flow.run() or set the environment "
                "variable BUILDFLOW_USAGE_STATS_DISABLE."
            )
            response = requests.post(
                "https://apis.launchflow.com/buildflow_usage",
                data=json.dumps(dataclasses.asdict(self._session)),
            )
            if response.status_code == 200:
                logging.debug("recorded run in session %s", self._session)
            else:
                logging.debug("failed to record usage stats.")
        print("Starting Flow Runtime")

        if enable_resource_creation:
            print("Setting up resources...")
            for proc in self._processors.values():
                proc.source.setup()
                proc.sink.setup(
                    process_arg_spec=proc.processor_instance.processor_arg_spec()
                )
            print("...Finished setting up resources")

        try:
            return self._run(streaming_options, node_name=node_name, blocking=blocking)
        except Exception as e:
            print("Flow failed with error: ", e)
            traceback.print_exc()
            raise e
        finally:
            # Reset the processors after each run. This may cause issues if
            # folks call run multiple times within a run. But it feels a more
            # straight forward.
            self._reset()

    def _reset(self):
        # TODO: Add support for multiple node types (i.e. endpoints).
        self._processors = {}

    def _run(
        self,
        streaming_options: options.StreamingOptions,
        node_name: str,
        blocking: bool,
    ):
        results: _BatchResults | _StreamingResults = None
        for proc_id, processor_ref in self._processors.items():
            proc_arg_spec = processor_ref.processor_instance.processor_arg_spec()
            if not proc_arg_spec.args:
                raise ValueError("Processor must have at least one argument.")
            proc_input_type = proc_arg_spec.annotations.get(proc_arg_spec.args[0], None)
            if not processor_ref.source.is_streaming():
                if results is None:
                    results = _BatchResults(node_name)
                manager = batch_manager.BatchProcessManager(
                    processor_ref, proc_input_type
                )
                results._add_processor_task(proc_id, manager.run())
            else:
                if results is None:
                    results = _StreamingResults(node_name)
                manager = stream_manager.StreamProcessManager(
                    processor_ref, proc_id, streaming_options, proc_input_type
                )
                manager.run()
                results.add_manager(manager)

        if blocking:
            return asyncio.run(results.output())
        else:
            return results

    def register_processor(
        self, processor_instance: ProcessorAPI, processor_id: Optional[str] = None
    ):
        if processor_id is None:
            processor_id = processor_instance.__class__.__name__
        if processor_id in self._processors:
            logging.warning(
                f"Processor {processor_id} already registered. Overwriting."
            )

        # Flows can only contain batch or streaming pipelines.
        if processor_instance.source().is_streaming() and any(
            [not p.source.is_streaming() for p in self._processors.values()]
        ):
            raise ValueError("Flows can only contain batch or streaming processors.")
        elif not processor_instance.source().is_streaming() and any(
            [p.source.is_streaming() for p in self._processors.values()]
        ):
            raise ValueError("Flows can only contain batch or streaming processors.")

        if (
            not processor_instance.source().is_streaming() and self._processors
        ) and any([p.source.is_streaming() for p in self._processors.values()]):
            raise ValueError(
                "Flows containing a streaming processor are only allowed "
                "to have one processor."
            )

        # NOTE: This is where the source / sink lifecycle methods are executed.
        self._processors[processor_id] = _ProcessorRef(
            processor_instance, processor_instance.source(), processor_instance.sink()
        )
