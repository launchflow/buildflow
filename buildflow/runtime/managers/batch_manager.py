from buildflow.runtime.managers import processors
from buildflow.runtime.ray_io import empty_io


class BatchProcessManager:

    def __init__(self, processor_ref: processors.ProcessorRef) -> None:
        self.processor_ref = processor_ref

    def run(self):
        key = str(self.processor_ref.sink)
        if isinstance(self.processor_ref.sink, empty_io.EmptySink):
            key = 'local'
        processor_actor = processors.ProcessActor.options(
            num_cpus=self.processor_ref.processor_instance.num_cpus()).remote(
                self.processor_ref.get_processor_replica())
        sink_actor = self.processor_ref.sink.actor(
            processor_actor, self.processor_ref.source.is_streaming())

        source_actor = self.processor_ref.source.actor({key: sink_actor})
        return source_actor.run.remote()
