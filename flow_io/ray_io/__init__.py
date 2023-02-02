from typing import List

from flow_io import utils
from flow_io.ray_io import all_output
from flow_io.ray_io import bigquery
from flow_io.ray_io import pubsub
from flow_io.ray_io import empty
from flow_io.ray_io import redis_stream

_NODE_SPACE_TO_SOURCE = {
    'resource/storage/bigquery/table': bigquery.BigQueryInput,
    'resource/queue/redis/stream': redis_stream.RedisStreamInput,
    'resource/queue/pubsub': pubsub.PubSubSourceActor,
}
_NODE_SPACE_TO_SINK = {
    'resource/storage/bigquery/table': bigquery.BigQueryOutput,
    'resource/queue/redis/stream': redis_stream.RedisStreamOutput,
    'resource/queue/pubsub': pubsub.PubsubSinkActor,
}


def source(*args, **kwargs):
    node_info = utils._get_node_info(utils._get_node_launch_file())
    config = kwargs
    if not node_info.incoming_node_spaces:
        source = empty.Input
    elif len(node_info.incoming_node_spaces) > 1:
        raise ValueError(
            f'Multiple incoming nodes for node: `{node_info["nodeSpace"]}`. '
            'This is currently not supported.')
    else:
        incoming_node_space = node_info.incoming_node_spaces[0]
        node_space_removed_id = '/'.join(incoming_node_space.split('/')[0:-1])
        incoming_node_info = utils._get_node_info(incoming_node_space)
        config.update(incoming_node_info.node_config)
        try:
            source = _NODE_SPACE_TO_SOURCE[node_space_removed_id]
        except KeyError:
            raise ValueError(
                f'IO is currently not supported for {node_space_removed_id}')
    return source.remote(args, node_info.node_space, **config)


def sink() -> List:
    node_info = utils._get_node_info(utils._get_node_launch_file())
    output_destinations = []
    if not node_info.outgoing_node_spaces:
        output_destinations.append((empty.Output, {}))
    else:
        for outgoing in node_info.outgoing_node_spaces:
            outgoing_node_space = outgoing
            node_space_removed_id = '/'.join(
                outgoing_node_space.split('/')[0:-1])
            outgoing_node_info = utils._get_node_info(outgoing_node_space)
            config = outgoing_node_info.node_config
            try:
                sink = _NODE_SPACE_TO_SINK[node_space_removed_id]
            except KeyError:
                raise ValueError('IO is currently not supported for '
                                 f'{node_space_removed_id}')
            output_destinations.append((sink, config))

    sinks = []
    for output_destination in output_destinations:
        sink, config = output_destination
        sinks.append(sink.remote(**config))
    return all_output.AllOutputActor.remote(sinks)
