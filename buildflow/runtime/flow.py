from typing import Optional

from buildflow.api import ProcessorAPI, flow, SourceType, SinkType, options
from buildflow.runtime.processor import processor
from buildflow.runtime.runner import Runtime


class Flow(flow.FlowAPI):
    _instance = None
    _initialized = False

    def __init__(self, name: str = '') -> None:
        if self._initialized:
            return
        self._initialized = True
        self._name = name
        self.runtime = Runtime()
        self.resources = set()
        self.processors = set()

    @classmethod
    def instance(cls):
        if cls._instance is None:
            raise ValueError('Flow has not yet been initialized. Did you call '
                             'buildflow.Flow(...)?')
        return cls._instance

    # This method is used to make this class a singleton
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def processor(self,
                  source: SourceType,
                  sink: Optional[SinkType] = None,
                  num_cpus: float = .5):
        return processor(self.runtime, source, sink, num_cpus)

    def run(
        self,
        processor_instance: Optional[ProcessorAPI] = None,
        *,
        streaming_options: options.StreamingOptions = options.StreamingOptions(
        ),
        disable_usage_stats: bool = False,
        enable_resource_creation: bool = True,
    ) -> flow.FlowResults:
        if processor_instance is not None:
            self.runtime.register_processor(processor_instance)
        return self.runtime.run(
            streaming_options=streaming_options,
            disable_usage_stats=disable_usage_stats,
            enable_resource_creation=enable_resource_creation)
