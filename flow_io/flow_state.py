import dataclasses
import inspect
import json
import os
import sys

from typing import Any, Dict, List, TypeVar


class InputOutput:
    """Super class for all input and output types."""
    _io_type: str

    @classmethod
    def from_config(cls, node_info: Dict[str, Any]):
        io_type = node_info['io_type']
        return _IO_MAPPING[io_type](**node_info)


IO = TypeVar('IO', bound=InputOutput)


@dataclasses.dataclass
class NodeState:
    input_ref: IO
    output_refs: List[IO]


@dataclasses.dataclass
class FlowState:
    # Mapping from entry point file to NodeState.
    node_states: Dict[str, NodeState]


@dataclasses.dataclass
class PubSub(IO):
    topic: str
    subscriber: str = ''
    _io_type: str = 'PUBSUB'


@dataclasses.dataclass
class BigQuery(IO):
    project: str = ''
    dataset: str = ''
    table: str = ''
    query: str = ''
    _io_type: str = 'BIG_QUERY'


_IO_MAPPING = {
    'BIG_QUERY': BigQuery,
    'PUBSUB': PubSub,
}


FLOW_STATE_ENV_VAR_NAME = 'FLOW_STATE_FILE'
_DEFAULT_FLOW_STATE_FILE = '/tmp/flow_io/flow_state.json'


def init(config: Dict[str, Any]):
    if not config:
        if FLOW_STATE_ENV_VAR_NAME not in os.environ:
            raise ValueError(
                'Either provide a config to flow_io.init() or use '
                'LaunchFlow to generate this config for you.')
        return
    frm = inspect.stack()[1]
    mod = inspect.getmodule(frm[0])
    if sys.version_info[1] <= 8:
        # py 3.8 only returns the relative file path.
        return os.path.join(os.getcwd(), mod.__file__)
    entry_point = mod.__file__
    node_input = config['input']
    node_outpus = config['outputs']
    node_state = NodeState(node_input, node_outpus)

    if FLOW_STATE_ENV_VAR_NAME in os.environ:
        with open(os.environ[FLOW_STATE_ENV_VAR_NAME], 'r',
                  encoding='UTF-8') as f:
            flow_state_dict = json.load(f)
            flow_state = FlowState(flow_state_dict['node_states'])
        if entry_point in flow_state.node_states:
            raise ValueError(
                'flow_io.init() should only be called once per file. '
                f'Was called multiple times from {entry_point}')
        flow_state.node_states[entry_point] = node_state
    else:
        os.environ[FLOW_STATE_ENV_VAR_NAME] = _DEFAULT_FLOW_STATE_FILE
        flow_state = FlowState({entry_point: node_state})
    with open(os.environ[FLOW_STATE_ENV_VAR_NAME], 'w', encoding='UTF-8') as f:
        json.dump(dataclasses.asdict(flow_state), f)
