"""Ray Actor output that writes to all configured sinks"""

from typing import Dict

import ray


@ray.remote
class AllOutputActor:

    def __init__(self, sinks):
        self.sinks = sinks

    def write(self, element: Dict):
        ray.get([sink.write.remote(element) for sink in self.sinks])