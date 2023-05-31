import asyncio
import dataclasses
import signal
import traceback
from typing import Iterable, List

from buildflow.api import NodeRunResult
from buildflow.core.managers import batch_manager, stream_manager
from buildflow.core.processor import Processor


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
