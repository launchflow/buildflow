# import os
import traceback
from typing import Dict, Iterable
import dataclasses
from buildflow.api import resources, ProcessorAPI
from buildflow.runtime.ray_io import (bigquery_io, duckdb_io, empty_io,
                                      redis_stream_io, pubsub_io)
import ray
from ray.util import ActorPool

# TODO: Add support for other IO types.
_IO_TYPE_TO_SOURCE = {
    resources.BigQuery.__name__: bigquery_io.BigQuerySourceActor,
    resources.DuckDB.__name__: duckdb_io.DuckDBSourceActor,
    resources.Empty.__name__: empty_io.EmptySourceActor,
    resources.PubSub.__name__: pubsub_io.PubSubSourceActor,
    resources.RedisStream.__name__: redis_stream_io.RedisStreamInput,
}

# TODO: Add support for other IO types.
_IO_TYPE_TO_SINK = {
    resources.BigQuery.__name__: bigquery_io.BigQuerySinkActor,
    resources.DuckDB.__name__: duckdb_io.DuckDBSinkActor,
    resources.Empty.__name__: empty_io.EmptySinkActor,
    resources.PubSub.__name__: pubsub_io.PubsubSinkActor,
    resources.RedisStream.__name__: redis_stream_io.RedisStreamOutput,
}


@dataclasses.dataclass
class _ProcessorRef:
    processor_class: type
    input_ref: type
    output_ref: type


@ray.remote
class _ProcessActor(object):

    def __init__(self, processor_class):
        self._processor: ProcessorAPI = processor_class()
        print(f'Running processor setup: {self._processor.__class__}')
        self._processor._setup()

    # TODO: Add support for process_async
    def process(self, *args, **kwargs):
        return self._processor.process(*args, **kwargs)

    def process_batch(self, calls: Iterable):
        to_ret = []
        for call in calls:
            to_ret.append(self.process(call))
        return to_ret


class Runtime:
    # NOTE: This is the singleton class.
    _instance = None
    _initialized = False

    def __init__(self):
        # TODO: Flesh this class out
        # _ = os.environ['FLOW_CONFIG']
        # TODO: maybe this should be max_io_replicas? For reading from bigquery
        # the API will use less replicas if it's smaller data.
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
            output = self._run()
            # Reset the p
            return output
        except Exception as e:
            print('Flow failed with error:')
            raise e
        finally:
            # Reset the processors after each run. This may cause issues if
            # folks call run multiple times within a run. But it feels a more
            # straight forward.
            self._processors = {}

    def _run(self):
        # TODO: Support multiple processors
        processor_ref = list(self._processors.values())[0]

        source_actor_class = _IO_TYPE_TO_SOURCE[
            processor_ref.input_ref.__class__.__name__]
        sink_actor_class = _IO_TYPE_TO_SINK[
            processor_ref.output_ref.__class__.__name__]
        source_inputs = source_actor_class.source_inputs(
            processor_ref.input_ref, self._config['num_io_replicas'])
        sources = []
        for inputs in source_inputs:
            processor_actor = _ProcessActor.remote(
                processor_ref.processor_class)
            sink = sink_actor_class.remote(
                processor_actor.process_batch.remote, processor_ref.output_ref)
            sources.append(
                # TODO: probably need support for unique keys. What if someone
                # writes to two bigquery tables?
                source_actor_class.remote(
                    {str(processor_ref.output_ref): sink}, *inputs))

        source_pool = ActorPool(sources)
        all_actor_outputs = list(
            source_pool.map(lambda actor, _: actor.run.remote(),
                            [None for _ in range(len(sources))]))
        final_output = {}
        for actor_output in all_actor_outputs:
            if actor_output is not None:
                for key, value in actor_output.items():
                    if key in final_output:
                        final_output[key].extend(value)
                    else:
                        final_output[key] = value
        return final_output

    def register_processor(self, processor_class: type,
                           input_ref: resources.IO, output_ref: resources.IO):
        print('DO NOT SUBMIT: ', self._processors)
        if len(self._processors) > 0:
            raise RuntimeError(
                'The Runner API currently only supports a single processor.')
        self._processors[processor_class.__name__] = _ProcessorRef(
            processor_class, input_ref, output_ref)
