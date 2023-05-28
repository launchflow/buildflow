import inspect
from typing import Iterable

from buildflow.api import (NodeApplyResult, NodeDestroyResult, NodePlan,
                           ProcessorPlan)
from buildflow.core.processor import Processor


class Infrastructure:

    def __init__(self) -> None:
        self._state = {}
        self._cache = {}

    def plan(self, node_name: str,
             node_processors: Iterable[Processor]) -> NodePlan:
        processor_plans = []
        for processor in node_processors:
            processor_arg_spec = inspect.getfullargspec(processor.process)
            # Create a plan for the Processor's Source
            source_provider = processor.source().provider()
            source_plan = source_provider.plan(processor_arg_spec)
            # Create a plan for the Processor's Sink (if it has one)
            sink_provider = processor.sink().provider()
            sink_plan = sink_provider.plan(processor_arg_spec)
            # Create a ProcessorPlan
            # TODO: Support multiple .sinks()
            processor_plans.append(
                ProcessorPlan(processor.name,
                              source_resources=[source_plan],
                              sink_resources=[sink_plan]))
        return NodePlan(node_name, processor_plans)

    def apply(self, node_name: str,
              node_processors: Iterable[Processor]) -> NodeApplyResult:
        plan = self.plan(node_name, node_processors)
        print(f'DEBUG - Infrastructure.apply - plan: {plan}')

    def destroy(self, node_name: str,
                node_processors: Iterable[Processor]) -> NodeDestroyResult:
        plan = self.plan(node_name, node_processors)
        print(f'DEBUG - Infrastructure.destroy - plan: {plan}')
