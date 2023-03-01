import dataclasses
import logging
import traceback
from typing import Dict, Iterable, Optional

import ray
from buildflow.api import ProcessorAPI, StorageAPI, resources
from buildflow.runtime.ray_io import (bigquery_io, duckdb_io, empty_io,
                                      pubsub_io, redis_stream_io)

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
        if self._initialized:
            return
        self._initialized = True

        self._processor_refs: Dict[str, _ProcessorRef] = {}
        self._storage_refs: Dict[str, StorageAPI] = {}

    # This method is used to make this class a singleton
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def run(self, num_replicas: int):
        print('Starting Flow Runtime')

        try:
            output = self._run(num_replicas)
            print('Flow finished successfully')
            return output
        except Exception as e:
            print('Flow failed with error: ', e)
            traceback.print_exc()
            raise e
        finally:
            # Reset the processors after each run. This may cause issues if
            # folks call run multiple times within a run. But it feels a more
            # straight forward.
            self._reset()

    def _reset(self):
        # TODO: Add support for multiple node types (i.e. endpoints).
        self._processor_refs = {}
        self._storage_refs = {}

    def _run(self, num_replicas: int):
        # TODO: Support multiple processors
        processor_ref = list(self._processor_refs.values())[0]

        # TODO: Add comments to explain this code, its pretty dense with
        # need-to-know info.
        source_actor_class = _IO_TYPE_TO_SOURCE[
            processor_ref.input_ref.__class__.__name__]
        sink_actor_class = _IO_TYPE_TO_SINK[
            processor_ref.output_ref.__class__.__name__]
        source_args = source_actor_class.source_args(processor_ref.input_ref,
                                                     num_replicas)
        source_pool_tasks = []
        for args in source_args:
            processor_actor = _ProcessActor.remote(
                processor_ref.processor_class)
            sink = sink_actor_class.remote(
                processor_actor.process_batch.remote, processor_ref.output_ref)
            # TODO: probably need support for unique keys. What if someone
            # writes to two bigquery tables?
            source = source_actor_class.remote(
                {str(processor_ref.output_ref): sink}, *args)
            num_threads = source_actor_class.recommended_num_threads()
            source_pool_tasks.extend(
                [source.run.remote() for _ in range(num_threads)])

        # We no longer need to use the Actor Pool because there's no input to
        # the actors (they spawn their own inputs based on the IO refs).
        # We also need to await each actor's subtask separately because its
        # now running on multiple threads.
        all_actor_outputs = ray.get(source_pool_tasks)

        # TODO: Add option to turn this off for prod deployments
        # Otherwise I think we lose time to sending extra data over the wire.
        final_output = {}
        for actor_output in all_actor_outputs:
            if actor_output is not None:
                for key, value in actor_output.items():
                    if key in final_output:
                        final_output[key].extend(value)
                    else:
                        final_output[key] = value
        return final_output

    def register_processor(self,
                           processor_class: type,
                           input_ref: resources.IO,
                           output_ref: resources.IO,
                           processor_id: Optional[str] = None):
        if processor_id is None:
            processor_id = processor_class.__qualname__
        if processor_id in self._processor_refs:
            logging.warning(
                f'Processor {processor_id} already registered. Overwriting.')
        # TODO: Support multiple processors
        elif len(self._processor_refs) > 0:
            raise RuntimeError(
                'The Runner API currently only supports a single processor.')

        self._processor_refs[processor_id] = _ProcessorRef(
            processor_class, input_ref, output_ref)

    def register_storage(self, storage_class: StorageAPI, storage_id: str):
        self._storage_refs[storage_id] = storage_class


def run(processor_class: Optional[type] = None, num_replicas: int = 1):
    runtime = Runtime()
    if processor_class is not None:
        runtime.register_processor(processor_class, processor_class._input(),
                                   processor_class._output())
    return runtime.run(num_replicas)
