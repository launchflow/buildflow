"""IO for dags that don't have any input / output configured."""

import dataclasses
import logging
from typing import Any, Callable, Iterable, List

import ray

from buildflow.api import io
from buildflow.runtime.ray_io import base


@dataclasses.dataclass
class EmptySource(io.Source):
    inputs: List[Any]

    def actor(self, ray_sinks):
        return EmptySourceActor.remote(ray_sinks, self)


@dataclasses.dataclass
class EmptySink(io.Sink):

    def actor(self, remote_fn: Callable, is_streaming: bool):
        return EmptySinkActor.remote(remote_fn)


@ray.remote(num_cpus=EmptySource.num_cpus())
class EmptySourceActor(base.RaySource):

    def __init__(
        self,
        ray_sinks: Iterable[base.RaySink],
        empty_ref: EmptySource,
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


@ray.remote(num_cpus=EmptySink.num_cpus())
class EmptySinkActor(base.RaySink):

    async def _write(
        self,
        elements: Iterable[Any],
    ):
        return elements
