from typing import List

from flow_io import utils
from flow_io.ray_io import bigquery
from flow_io.ray_io import pubsub
from flow_io.ray_io import empty
from flow_io.ray_io import redis_stream

_NODE_SPACE_TO_INPUT = {
    'resource/storage/bigquery/table': bigquery.BigQueryInput,
    'resource/queue/redis/stream': redis_stream.RedisStreamInput,
    'resource/queue/pubsub': pubsub.PubSubInput,
}
_NODE_SPACE_TO_OUTPUT = {
    'resource/storage/bigquery/table': bigquery.BigQueryOutput,
    'resource/queue/redis/stream': redis_stream.RedisStreamOutput,
    'resource/queue/pubsub': pubsub.PubSubOutput,
}


def input(*args, **kwargs):
    node_info = utils._get_node_info(utils._get_node_space_from_module())
    config = kwargs
    if not node_info.incoming_node_spaces:
        dag_input = empty.Input
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
            dag_input = _NODE_SPACE_TO_INPUT[node_space_removed_id]
        except KeyError:
            raise ValueError(
                f'IO is currently not supported for {node_space_removed_id}')
    dag_input(args, **config).run()


def output(*args) -> List:
    node_info = utils._get_node_info(utils._get_node_space_from_module())
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
                dag_output = _NODE_SPACE_TO_OUTPUT[node_space_removed_id]
            except KeyError:
                raise ValueError(
                    'IO is currently not supported for '
                    f'{node_space_removed_id}'
                )
            output_destinations.append((dag_output, config))

    final_outputs = []
    for output_destination in output_destinations:
        dag_output, config = output_destination
        for output in args:
            output_class = dag_output.bind(**config)
            final_outputs.append(output_class.write.bind(output))
    return final_outputs
