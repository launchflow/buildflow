import asyncio
import dataclasses
import signal
import traceback
from typing import Iterable, List

from buildflow.api import NodeRunResult
from buildflow.runtime.managers import batch_manager, stream_manager
from buildflow.runtime.processor import Processor


@dataclasses.dataclass
class _StreamingResult(NodeRunResult):

    def __init__(self, node_name: str) -> None:
        super().__init__(node_name)
        self._managers = []
        self._manager_tasks = []

    def add_manager(self, manager: stream_manager.StreamProcessManager):
        manager.run()
        self._managers.append(manager)
        self._manager_tasks.append(manager.manager_task)

    async def output(self, register_drain: bool = True):
        if register_drain:
            loop = asyncio.get_event_loop()
            for signame in ("SIGINT", "SIGTERM"):
                loop.add_signal_handler(
                    getattr(signal, signame),
                    lambda: asyncio.create_task(self.drain()),
                )
        return await asyncio.gather(*self._manager_tasks)

    async def drain(self, *args):
        print(f"Draining node: {self.node_name}...")
        drain_tasks = []
        for manager in self._managers:
            # same time.
            drain_tasks.append(manager.drain())
        await asyncio.gather(*drain_tasks)
        await asyncio.gather(*self._manager_tasks)
        print(f"...node: {self.node_name} shut down.")


@dataclasses.dataclass
class _BatchResult(NodeRunResult):

    def __init__(self, node_name: str) -> None:
        super().__init__(node_name)
        self._processor_tasks = {}

    def add_manager(self, processor_id: str,
                    manager: batch_manager.BatchProcessManager):
        self._processor_tasks[processor_id] = manager.run()

    async def output(self):
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


@dataclasses.dataclass
class _CombinedResult(NodeRunResult):

    def __init__(self, node_name: str,
                 results: Iterable[NodeRunResult]) -> None:
        super().__init__(node_name)
        for result in results:
            if result.node_name != node_name:
                raise ValueError(
                    'Cannot combine results from different nodes: '
                    f'{result.node_name} != {node_name}')
        self._results: results

    async def output(self):
        return await asyncio.gather(
            *[result.output() for result in self._results])


class Runtime:

    def run(self,
            *,
            node_name: str,
            node_processors: Iterable[Processor],
            blocking: bool = True):

        try:
            return self._run(node_name=node_name,
                             node_processors=node_processors,
                             blocking=blocking)
        except Exception as e:
            print("Flow failed with error: ", e)
            traceback.print_exc()
            raise e

    def _run(
        self,
        node_name: str,
        node_processors: Iterable[Processor],
        blocking: bool,
    ) -> _CombinedResult:
        all_results: List[NodeRunResult] = []
        for processor in node_processors:

            if not processor.source().is_streaming():
                batch_result = _BatchResult(node_name)
                manager = batch_manager.BatchProcessManager(processor)
                batch_result.add_manager(processor.name, manager)
                all_results.append(batch_result)
            else:
                streaming_result = _StreamingResult(node_name)
                manager = stream_manager.StreamProcessManager(
                    processor, processor.name)
                streaming_result.add_manager(manager)
                all_results.append(streaming_result)

        combined_result = _CombinedResult(node_name, all_results)
        if blocking:
            return asyncio.run(combined_result.output())
        else:
            return combined_result
