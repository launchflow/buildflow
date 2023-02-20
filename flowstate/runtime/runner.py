# import os
from typing import Dict
import dataclasses
from flowstate.api import resources, ProcessorAPI
from flowstate.runtime.ray_io import (bigquery_io, duckdb_io, empty_io,
                                      pubsub_io)
import ray
from ray.util import ActorPool

# TODO: Add support for other IO types.
_IO_TYPE_TO_SOURCE = {
    resources.BigQuery.__name__: bigquery_io.BigQuerySourceActor,
    resources.DuckDB.__name__: duckdb_io.DuckDBSourceActor,
    resources.Empty.__name__: empty_io.EmptySourceActor,
    resources.PubSub.__name__: pubsub_io.PubSubSourceActor,
}

# TODO: Add support for other IO types.
_IO_TYPE_TO_SINK = {
    resources.BigQuery.__name__: bigquery_io.BigQuerySinkActor,
    resources.DuckDB.__name__: duckdb_io.DuckDBSinkActor,
    resources.Empty.__name__: empty_io.EmptySinkActor,
    resources.PubSub.__name__: pubsub_io.PubsubSinkActor,
}


@dataclasses.dataclass
class _ProcessorRef:
    processor_class: type
    input_ref: type
    output_ref: type


@ray.remote
class _ProcessActor(object):

    def __init__(self, processor_class: type):
        self._processor: ProcessorAPI = processor_class()
        print(f'Running processor setup: {self._processor.__class__}')
        self._processor._setup()

    # TODO: Add support for process_async
    def process(self, *args, **kwargs):
        return self._processor.process(*args, **kwargs)


class Runtime:
    # NOTE: This is the singleton class.
    _instance = None
    _initialized = False

    def __init__(self):
        # TODO: Flesh this class out
        # _ = os.environ['FLOW_CONFIG']
        self._config = {
            'num_io_replicas': 1,
        }

        if self._initialized:
            return
        self._initialized = True

        self._processors: Dict[str, _ProcessorRef] = {}

    # This method is used to make this class a singleton
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def run(self):
        print('Starting Flow Runtime')

        try:
            self._run()
        except Exception as e:
            print(f'Flow failed with error: {e}')

        print('Flow completed successfully!')

    def _run(self):
        # TODO: Support multiple processors
        processor_ref = list(self._processors.values())[0]

        processor_actor = _ProcessActor.remote(processor_ref.processor_class)

        all_source_actors = []
        for _ in range(self._config['num_io_replicas']):
            sink_actor = _IO_TYPE_TO_SINK[
                processor_ref.output_ref.__class__.__name__].remote(
                    processor_actor.process.remote, processor_ref.output_ref)
            source_actor = _IO_TYPE_TO_SOURCE[
                processor_ref.input_ref.__class__.__name__].remote(
                    [sink_actor], processor_ref.input_ref)
            all_source_actors.append(source_actor)

        source_pool = ActorPool(all_source_actors)
        return list(
            source_pool.map(
                lambda actor, _: actor.run.remote(),
                [None for _ in range(self._config['num_io_replicas'])]))

    def register_processor(self, processor_class: type,
                           input_ref: resources.IO, output_ref: resources.IO):
        if len(self._processors) > 0:
            raise RuntimeError(
                'The Runner API currently only supports a single processor.')
        self._processors[processor_class.__name__] = _ProcessorRef(
            processor_class, input_ref, output_ref)
