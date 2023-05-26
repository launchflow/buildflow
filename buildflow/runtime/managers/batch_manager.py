from typing import Optional, Type

from buildflow.runtime.managers import processors
from buildflow.runtime.ray_io import empty_io
from buildflow import Processor


class BatchProcessManager:

    def __init__(self, processor: Processor) -> None:
        self.processor = processor

    def run(self):
        key = str(self.processor_ref.sink)
        if isinstance(self.processor_ref.sink, empty_io.EmptySink):
            key = "local"
        processor_actor = processors.ProcessActor.options(
            num_cpus=self.processor_ref.processor_instance.num_cpus()).remote(
                self.processor_ref.get_processor_replica())
        sink_actor = self.processor_ref.sink.actor(
            processor_actor, self.processor_ref.source.is_streaming())

        source_actor = self.processor_ref.source.actor({key: sink_actor},
                                                       self.proc_input_type)
        return source_actor.run.remote()
