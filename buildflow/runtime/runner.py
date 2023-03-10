import argparse
import copy
import dataclasses
import json
import logging
import os
import sys
import traceback
from typing import Dict, Iterable, Optional

import ray
import requests

from buildflow import utils
from buildflow.api import IO, ProcessorAPI, io
from buildflow.runtime.ray_io import (bigquery_io, duckdb_io, empty_io,
                                      pubsub_io, redis_stream_io)

# TODO: Add support for other IO types.
_IO_TYPE_TO_SOURCE = {
    io.BigQuery.__name__: bigquery_io.BigQuerySourceActor,
    io.DuckDB.__name__: duckdb_io.DuckDBSourceActor,
    io.Empty.__name__: empty_io.EmptySourceActor,
    io.PubSub.__name__: pubsub_io.PubSubSourceActor,
    io.RedisStream.__name__: redis_stream_io.RedisStreamInput,
}

# TODO: Add support for other IO types.
_IO_TYPE_TO_SINK = {
    io.BigQuery.__name__: bigquery_io.BigQuerySinkActor,
    io.DuckDB.__name__: duckdb_io.DuckDBSinkActor,
    io.Empty.__name__: empty_io.EmptySinkActor,
    io.PubSub.__name__: pubsub_io.PubsubSinkActor,
    io.RedisStream.__name__: redis_stream_io.RedisStreamOutput,
}


@dataclasses.dataclass
class _ProcessorRef:
    processor_instance: ProcessorAPI
    source: IO
    sink: IO

    def get_processor_replica(self):
        return copy.deepcopy(self.processor_instance)


_SESSION_DIR = os.path.join(os.path.expanduser('~'), '.config', 'buildflow')
_SESSION_FILE = os.path.join(_SESSION_DIR, 'build_flow_usage.json')


@dataclasses.dataclass
class Session:
    id: str


@ray.remote
class _ProcessActor(object):

    def __init__(self, processor_instance: ProcessorAPI):
        self._processor = processor_instance
        print(f'Running processor setup: {self._processor.__class__}')
        # NOTE: This is where the setup lifecycle method is called.
        self._processor.setup()

    def process(self, *args, **kwargs):
        return self._processor.process(*args, **kwargs)

    def process_batch(self, calls: Iterable):
        to_ret = []
        for call in calls:
            to_ret.append(self.process(call))
        return to_ret


def _load_session():
    try:
        os.makedirs(_SESSION_DIR, exist_ok=True)
        if os.path.exists(_SESSION_FILE):
            with open(_SESSION_FILE, 'r') as f:
                session_info = json.load(f)
                return Session(**session_info)
        else:
            session = Session(id=utils.uuid())
            with open(_SESSION_FILE, 'w') as f:
                json.dump(dataclasses.asdict(session), f)
            return session
    except Exception as e:
        logging.debug('failed to load session id with error: %s', e)


class Runtime:

    def __init__(self):
        self._processors: Dict[str, _ProcessorRef] = {}
        self._session = _load_session()
        parser = argparse.ArgumentParser()
        parser.add_argument('--disable_usage_stats',
                            action='store_true',
                            default=False)
        args, _ = parser.parse_known_args(sys.argv)
        self._enable_usage = True
        if args.disable_usage_stats:
            self._enable_usage = False

    def run(self, num_replicas: int):
        if self._enable_usage:
            print(
                'Usage stats collection is enabled. To disable add the flag: '
                '`--disable_usage_stats`.')
            response = requests.post(
                'https://apis.launchflow.com/buildflow_usage',
                data=json.dumps(dataclasses.asdict(self._session)))
            if response.status_code == 200:
                logging.debug('recorded run in session %s', self._session)
            else:
                logging.debug('failed to record usage stats.')
        print('Starting Flow Runtime')

        print('Setting up resources...')
        for proc in self._processors.values():
            proc.source.setup()
            proc.sink.setup()
        print('...Finished setting up resources')

        try:
            output = self._run(num_replicas)
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
        self._processors = {}

    def _run(self, num_replicas: int):
        running_tasks = {}
        source_actors = {}
        for proc_id, processor_ref in self._processors.items():
            # TODO: Add comments to explain this code, its pretty dense with
            # need-to-know info.
            source_actor_class = _IO_TYPE_TO_SOURCE[
                processor_ref.source.__class__.__name__]
            sink_actor_class = _IO_TYPE_TO_SINK[
                processor_ref.sink.__class__.__name__]
            source_args = source_actor_class.source_args(
                processor_ref.source, num_replicas)
            source_pool_tasks = []
            for args in source_args:
                processor_actor = _ProcessActor.remote(
                    processor_ref.get_processor_replica())
                sink = sink_actor_class.remote(
                    processor_actor.process_batch.remote, processor_ref.sink)
                # TODO: probably need support for unique keys. What if someone
                # writes to two bigquery tables?
                key = str(processor_ref.sink)
                if isinstance(processor_ref.sink, io.Empty):
                    key = 'local'
                source = source_actor_class.remote({key: sink}, *args)
                source_actors[proc_id] = source
                num_threads = source_actor_class.recommended_num_threads()
                source_pool_tasks.extend(
                    [source.run.remote() for _ in range(num_threads)])

            # We no longer need to use the Actor Pool because there's no input
            # to the actors (they spawn their own inputs based on the IO refs).
            # We also need to await each actor's subtask separately because its
            # now running on multiple threads.
            running_tasks[proc_id] = source_pool_tasks

        final_output = {}
        for proc_id, tasks in running_tasks.items():
            try:
                all_actor_outputs = ray.get(tasks)
            except KeyboardInterrupt:
                print('Shutting down processors...')
                source = source_actors[proc_id]
                should_block = ray.get(source.shutdown.remote())
                if should_block:
                    ray.get(tasks)
                print('...Sucessfully shut down processors.')
                return

            # TODO: Add option to turn this off for prod deployments
            # Otherwise I think we lose time to sending extra data over the
            # wire.
            processor_output = {}
            for actor_output in all_actor_outputs:
                if actor_output is not None:
                    for key, value in actor_output.items():
                        if key in processor_output:
                            processor_output[key].extend(value)
                        else:
                            processor_output[key] = value
            final_output[proc_id] = processor_output
        return final_output

    def register_processor(self,
                           processor_instance: ProcessorAPI,
                           processor_id: Optional[str] = None):
        if processor_id is None:
            processor_id = processor_instance.__class__.__name__
        if processor_id in self._processors:
            logging.warning(
                f'Processor {processor_id} already registered. Overwriting.')

        # NOTE: This is where the source / sink lifecycle methods are executed.
        self._processors[processor_id] = _ProcessorRef(
            processor_instance, processor_instance.source(),
            processor_instance.sink())
