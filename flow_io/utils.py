import dataclasses
import inspect
import json
import os
from typing import Any, Dict


def _get_flow_file() -> str:
    flow_file = os.environ.get('FLOW_FILE')
    if flow_file is None:
        # TODO: maybe we could try and parse this out if it's not set? My main
        # worry is that it feels brittle and might break easily
        # Maybe we can walk backwards till we find the correct file?
        raise ValueError(
            'Could not determine flow file. Please set the FLOW_FILE '
            'environment variable to point to the flow that is running. If you'
            ' using the LaunchFlow extension this should happen automatically.'
        )
    return flow_file


def _get_deployment_file() -> str:
    deployment_file = os.environ.get('FLOW_DEPLOYMENT_FILE')
    if deployment_file is None:
        # TODO: maybe we could try and parse this out if it's not set? My main
        # worry is that it feels brittle and might break easily
        # Maybe we can walk backwards till we find the correct file?
        raise ValueError(
            'Could not determine deployment file. Please set the '
            'FLOW_DEPLOYMENT_FILE environment variable to point to the flow '
            'that is running. If you using the LaunchFlow extension this '
            'should happen automatically.')
    return deployment_file


def _read_flow_config() -> Dict[str, Any]:
    flow_file = _get_flow_file()
    with open(flow_file, 'r', encoding='UTF-8') as f:
        flow = json.load(f)
    return flow


def _read_deployment_config() -> Dict[str, Any]:
    deployment_file = _get_deployment_file()
    with open(deployment_file, 'r', encoding='UTF-8') as f:
        flow = json.load(f)
    return flow


def _get_node_space_from_module(depth: int = 1) -> str:
    for i, frm in enumerate(inspect.stack()):
        print(f'DO NOT SUBMIT: {i} {inspect.getmodule(frm[0])}')
    frm = inspect.stack()[depth]
    print('DO NOT SUBMIT: ', frm)
    mod = inspect.getmodule(frm[0])
    mod_name = mod.__name__
    return mod_name.replace('.', '/')


@dataclasses.dataclass
class _NodeInfo:
    node_space: str
    incoming_node_spaces: str
    outgoing_node_spaces: str
    node_config: str


def _get_node_info(node_space: str) -> _NodeInfo:
    flow = _read_flow_config()
    deployment = _read_deployment_config()
    node = None
    for n in flow['nodes']:
        if n['nodeSpace'] in node_space:
            node = n
            break
    if node is None:
        raise ValueError(
            f'Unable to find node for calling module: {node_space}')

    outgoing_node_spaces = flow['outgoingEdges'].get(node['nodeSpace'], [])
    incoming_node_spaces = []
    for incoming, nodes in flow['outgoingEdges'].items():
        if node['nodeSpace'] in nodes:
            incoming_node_spaces.append(incoming)
    config = deployment['nodeDeployments'].get(node_space, {})
    return _NodeInfo(node['nodeSpace'], incoming_node_spaces,
                     outgoing_node_spaces, config)
