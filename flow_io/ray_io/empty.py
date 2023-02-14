"""IO for dags that don't have any input / output configured."""

import logging
from typing import Any, Dict, Iterable

import ray

from flow_io.ray_io import base


@ray.remote
class EmptySourceActor(base.RaySource):

    def __init__(
        self,
        ray_inputs,
        node_space: str,
        inputs: Iterable[Any] = [],
    ) -> None:
        super().__init__(ray_inputs, node_space)
        self.inputs = inputs
        if not inputs:
            logging.warning(
                'No inputs provide your source. This means nothing will '
                'happen. You can pass inputs like: '
                '`ray_io.source(..., inputs=[1, 2, 3]`)')

    def run(self):
        refs = []
        for i in self.inputs:
            for ray_input in self.ray_inputs:
                refs.append(ray_input.remote(i))
        return ray.get(refs)


@ray.remote
class EmptySinkActor(base.RaySink):

    def __init__(self, node_space: str) -> None:
        super().__init__(node_space)

    def _write(self, element=Any, carrier: Dict[str, str] = {}):
        del carrier
        return element
