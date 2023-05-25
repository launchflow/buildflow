from typing import Optional

from buildflow.api import ProcessorAPI, SourceType, SinkType, node, options
from buildflow.runtime.processor import processor
from buildflow.runtime.runner import Runtime


class Node(node.NodeAPI):
    def __init__(self, name: str = "") -> None:
        super().__init__(name)
        self._runtime = Runtime()

    def processor(
        self,
        source: SourceType,
        sink: Optional[SinkType] = None,
        num_cpus: float = 0.5,
        autoscaling_options: options.AutoscalingOptions = options.AutoscalingOptions(),
    ):
        return processor(self._runtime, source, sink, num_cpus, autoscaling_options)

    def add_processor(self, processor: ProcessorAPI):
        self._runtime.register_processor(processor)

    def run(
        self,
        *,
        disable_usage_stats: bool = False,
        disable_resource_creation: bool = True,
        blocking: bool = True,
    ) -> node.NodeResults:
        if not disable_resource_creation:
            self._runtime.setup()
        return self._runtime.run(
            disable_usage_stats=disable_usage_stats,
            node_name=self.name,
            blocking=blocking,
        )

    def plan(self) -> node.NodePlan:
        return self._runtime.plan(node_name=self.name)

    def setup(self):
        return self._runtime.setup()
