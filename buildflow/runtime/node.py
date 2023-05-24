from typing import Optional

from buildflow.api import ProcessorAPI, SourceType, SinkType, node, options
from buildflow.runtime.processor import processor
from buildflow.runtime.runner import Runtime


class Node(node.NodeAPI):

    def __init__(self, name: str = "") -> None:
        super().__init__(name)
        self._runtime = Runtime()

    def processor(
        self, source: SourceType, sink: Optional[SinkType] = None, num_cpus: float = 0.5
    ):
        return processor(self._runtime, source, sink, num_cpus)
    
    def add_processor(self, processor: ProcessorAPI):
        self._runtime.register_processor(processor)

    def run(
        self,
        *,
        streaming_options: options.StreamingOptions = options.StreamingOptions(),
        disable_usage_stats: bool = False,
        enable_resource_creation: bool = True,
        blocking: bool = True,
    ) -> node.NodeResults:
        return self._runtime.run(
            streaming_options=streaming_options,
            disable_usage_stats=disable_usage_stats,
            enable_resource_creation=enable_resource_creation,
            node_name=self.name,
            blocking=blocking,
        )
