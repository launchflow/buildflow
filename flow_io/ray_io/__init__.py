from typing import List

from flow_io import utils
from flow_io.ray_io import empty
from flow_io.ray_io import redis_stream

_NODE_SPACE_TO_INPUT = {
    'resource/queue/redis/stream': redis_stream.RedisStreamInput
}
_NODE_SPACE_TO_OUTPUT = {
    'resource/queue/redis/stream': redis_stream.RedisStreamOutput
}


def input(*args, **kwargs):
    node_info = utils._get_node_info(utils._get_node_space_from_module())

    if not node_info.incoming_node_spaces:
        # TODO
        dag_input = empty.Input
        metadata = kwargs
    elif len(node_info.incoming_node_spaces) > 1:
        raise ValueError(
            f'Multiple incoming nodes for node: `{node_info["nodeSpace"]}`. '
            'This is currently not supported.')
    else:
        incoming_node_space = node_info.incoming_node_spaces[0]
        incoming_node_info = utils._get_node_info(incoming_node_space)
        metadata = incoming_node_info.node['metadata']
        try:
            dag_input = _NODE_SPACE_TO_INPUT[incoming_node_space]
        except KeyError:
            raise ValueError(
                f'IO is currently not supported for {incoming_node_space}')
    dag_input(args, **metadata).run()


def output(*args) -> List:
    node_info = utils._get_node_info(utils._get_node_space_from_module())
    if not node_info.outgoing_node_spaces:
        dag_output = empty.Output
        metadata = {}
    elif len(node_info.outgoing_node_spaces) > 1:
        raise ValueError(
            f'Multiple outgoing nodes for node: `{node_info["nodeSpace"]}`. '
            'This is currently not supported.')
    else:
        outgoing_node_space = node_info.outgoing_node_spaces[0]
        outgoing_node_info = utils._get_node_info(outgoing_node_space)
        metadata = outgoing_node_info.node['metadata']
        try:
            dag_output = _NODE_SPACE_TO_OUTPUT[outgoing_node_space]
        except KeyError:
            raise ValueError(
                f'IO is currently not supported for {outgoing_node_space}')

    final_outputs = []
    for output in args:
        output_class = dag_output.bind(**metadata)
        final_outputs.append(output_class.write.bind(output))
    return final_outputs
