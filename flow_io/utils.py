import inspect
import json
import os
import sys

from flow_io.flow_state import FLOW_STATE_ENV_VAR_NAME, FlowState, NodeState


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
