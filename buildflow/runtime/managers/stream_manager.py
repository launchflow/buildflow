import ray

from buildflow.api import options
from buildflow.runtime.managers import processors
from buildflow.runtime.ray_io import empty_io


@ray.remote
class _StreamManagerActor:

    def __init__(self, options: options.StreamingOptions,
                 processor_ref: processors.ProcessorRef) -> None:
        self.options = options
        self.processor_ref = processor_ref

    def _add_replica(self):
        key = str(self.processor_ref.sink)
        if isinstance(self.processor_ref.sink, empty_io.EmptySink):
            key = 'local'
        processor_actor = processors.ProcessActor.remote(
            self.processor_ref.get_processor_replica())
        sink_actor = self.processor_ref.sink.actor(
            processor_actor.process_batch.remote,
            self.processor_ref.source.is_streaming())

        source_actor = self.processor_ref.source.actor({key: sink_actor})
        return source_actor.run.remote()

    async def run(self):
        start_replics = self.options.min_replicas
        if self.options.num_replicas:
            start_replics = self.options.num_replicas
        replicas = [self._add_replica() for _ in range(start_replics)]
        while True:
            if not self.options.autoscaling:
                # no autoscaling so just let the replicas run.
                continue
            else:
                print(replicas)


class StreamProcessManager:

    def __init__(self, processor_ref: processors.ProcessorRef,
                 streaming_options: options.StreamingOptions) -> None:
        self.processor_ref = processor_ref
        self.options = streaming_options

    def run(self):
        actor = _StreamManagerActor.remote(self.options, self.processor_ref)
        return actor.run.remote()
