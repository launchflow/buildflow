from typing import Callable

import ray

from flow_io import flow_state, resources
from flow_io.ray_io import bigquery, duckdb, empty, pubsub, redis_stream

# TODO: Add support for other IO types.
_IO_TYPE_TO_SOURCE = {
    resources.BigQuery.__name__: bigquery.BigQuerySourceActor,
    resources.PubSub.__name__: pubsub.PubSubSourceActor,
    resources.Empty.__name__: empty.EmptySourceActor,
    resources.DuckDB.__name__: duckdb.DuckDBSourceActor,
    resources.RedisStream.__name__: redis_stream.RedisStreamInput,
}

# TODO: Add support for other IO types.
_IO_TYPE_TO_SINK = {
    resources.BigQuery.__name__: bigquery.BigQuerySinkActor,
    resources.PubSub.__name__: pubsub.PubsubSinkActor,
    resources.Empty.__name__: empty.EmptySinkActor,
    resources.DuckDB.__name__: duckdb.DuckDBSinkActor,
    resources.RedisStream.__name__: redis_stream.RedisStreamOutput,
}


def run(remote_fn: Callable):
    node_state = flow_state.get_node_state(flow_state.get_node_launch_file())
    print('DO NOT SUBMIT: here ', node_state.input_ref.__class__.__name__)

    sink_actors = [
        _IO_TYPE_TO_SINK[output_ref.__class__.__name__].remote(
            remote_fn, output_ref) for output_ref in node_state.output_refs
    ]
    source_actor = _IO_TYPE_TO_SOURCE[
        node_state.input_ref.__class__.__name__].remote(
            sink_actors, node_state.input_ref)

    return ray.get(source_actor.run.remote())
