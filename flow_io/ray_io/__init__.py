from typing import Callable

import ray

from flow_io import flow_state, resources
from flow_io.ray_io import bigquery, pubsub

# TODO: Add support for other IO types.
_IO_TYPE_TO_SOURCE = {
    resources.BigQuery.__name__: bigquery.BigQuerySourceActor,
    resources.PubSub.__name__: pubsub.PubSubSourceActor,
}

# TODO: Add support for other IO types.
_IO_TYPE_TO_SINK = {
    resources.BigQuery.__name__: bigquery.BigQuerySinkActor,
    resources.PubSub.__name__: pubsub.PubsubSinkActor,
}


def run(remote_fn: Callable):
    node_state = flow_state.get_node_state(flow_state.get_node_launch_file())
    sink_actors = [
        _IO_TYPE_TO_SINK[output_ref.__class__.__name__].remote(
            remote_fn, output_ref) for output_ref in node_state.output_refs
    ]
    source_actor = _IO_TYPE_TO_SOURCE[
        node_state.input_ref.__class__.__name__].remote(
            sink_actors, node_state.input_ref)
    ray.get(source_actor.run.remote())
