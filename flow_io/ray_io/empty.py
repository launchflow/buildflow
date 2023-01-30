"""IO for dags that don't have any input / output configured."""

import time
from typing import Any, Iterable

import ray


class Input:

    def __init__(self, ray_dags: Iterable, inputs: Iterable[Any] = []) -> None:
        self.ray_dags = ray_dags
        self.inputs = inputs

    def run(self):
        for i in self.inputs:
            time.sleep(1)
            for dag in self.ray_dags:
                dag.execute(i)


@ray.remote
class Output:

    def write(self, element=Any):
        print('OUTPUT: ', element)
