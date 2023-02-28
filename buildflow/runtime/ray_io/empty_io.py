"""IO for dags that don't have any input / output configured."""

import logging
from typing import Any, Callable, Dict, Iterable, Union

import ray

from buildflow.api import resources
from buildflow.runtime.ray_io import base


@ray.remote
class EmptySourceActor(base.RaySource):

    def __init__(
        self,
        ray_sinks: Iterable[base.RaySink],
        empty_ref: resources.Empty,
    ) -> None:
        super().__init__(ray_sinks)
        self.inputs = empty_ref.inputs
        if not self.inputs:
            logging.warning(
                'No inputs provide your source. This means nothing will '
                'happen. You can pass inputs like: '
                '`ray_io.source(..., inputs=[1, 2, 3]`)')

    async def run(self):
        return await self._send_batch_to_sinks_and_await(self.inputs)


@ray.remote
class EmptySinkActor(base.RaySink):

    def __init__(
        self,
        remote_fn: Callable,
        empty_ref: resources.Empty,
    ) -> None:
        super().__init__(remote_fn)

    async def _write(
        self,
        elements: Union[Dict[str, Any], Iterable[Dict[str, Any]]],
    ):
        return elements
