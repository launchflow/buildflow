import dataclasses
import inspect
import json
import os
import sys
import uuid
from typing import Any, Dict, List

from flow_io import resources


@dataclasses.dataclass
class NodeState:
    input_ref: resources.IO
    output_refs: List[resources.IO]


@dataclasses.dataclass
class FlowState:
    # Mapping from entry point file to NodeState.
    node_states: Dict[str, NodeState]

    @classmethod
    def from_config(cls, config: Dict[str, Any]):
        node_states = {}
        for entry_point, node_state in config['node_states'].items():
            node_states[entry_point] = NodeState(
                input_ref=resources.InputOutput.from_config(
                    node_state['input_ref']),
                output_refs=[
                    resources.InputOutput.from_config(output_ref)
                    for output_ref in node_state['output_refs']
                ])
        return FlowState(node_states)


FLOW_STATE_ENV_VAR_NAME = 'FLOW_STATE_FILE'
_DEFAULT_FLOW_IO_DIR = '/tmp/flow_io'


def default_flow_state_file():
    return os.path.join(_DEFAULT_FLOW_IO_DIR, f'{str(uuid.uuid4())}.json')


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
        entry_point = os.path.join(os.getcwd(), mod.__file__)
    else:
        entry_point = mod.__file__
    node_input = config['input']
    node_outpus = config['outputs']
    node_state = NodeState(node_input, node_outpus)

    if FLOW_STATE_ENV_VAR_NAME in os.environ:
        with open(os.environ[FLOW_STATE_ENV_VAR_NAME], 'r',
                  encoding='UTF-8') as f:
            flow_state_dict = json.load(f)
            flow_state = FlowState(flow_state_dict['node_states'])
        flow_state.node_states[entry_point] = node_state
    else:
        os.environ[FLOW_STATE_ENV_VAR_NAME] = default_flow_state_file()
        os.makedirs(_DEFAULT_FLOW_IO_DIR, exist_ok=True)
        flow_state = FlowState({entry_point: node_state})
    with open(os.environ[FLOW_STATE_ENV_VAR_NAME], 'w', encoding='UTF-8') as f:
        json.dump(dataclasses.asdict(flow_state), f)


def _get_flow_state_file() -> str:
    flow_state_file = os.environ.get(FLOW_STATE_ENV_VAR_NAME)
    if flow_state_file is None:
        # TODO: maybe we could try and parse this out if it's not set? My main
        # worry is that it feels brittle and might break easily
        # Maybe we can walk backwards till we find the correct file?
        raise ValueError(
            'Could not determine flow state file. Please set the '
            f'{FLOW_STATE_ENV_VAR_NAME} environment variable to point '
            'to the flow that is running. If you are using the LaunchFlow '
            'extension this should happen automatically.')
    return flow_state_file


def get_node_launch_file(depth: int = 2) -> str:
    frm = inspect.stack()[depth]
    mod = inspect.getmodule(frm[0])
    if sys.version_info[1] <= 8:
        # py 3.8 only returns the relative file path.
        return os.path.join(os.getcwd(), mod.__file__)
    return mod.__file__


def get_flow_state() -> FlowState:
    flow_file = _get_flow_state_file()
    with open(flow_file, 'r', encoding='UTF-8') as f:
        return FlowState.from_config(json.load(f))


def get_node_state(entrypoint_file: str) -> NodeState:
    flow_state = get_flow_state()
    if entrypoint_file not in flow_state.node_states:
        raise RuntimeError(
            f'{entrypoint_file} not found in FlowState config: {flow_state}')
    return flow_state.node_states[entrypoint_file]
