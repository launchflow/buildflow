import copy
import dataclasses
import time
from typing import Iterable

import ray
from ray.util.metrics import Gauge

from buildflow.api import SinkType, SourceType, ProcessorAPI


@dataclasses.dataclass
class ProcessorRef:
    processor_instance: ProcessorAPI
    source: SourceType
    sink: SinkType

    def get_processor_replica(self):
        return copy.deepcopy(self.processor_instance)


# TODO(#113): make this configurable by the user
@ray.remote
class ProcessActor(object):

    def __init__(self, processor_instance: ProcessorAPI):
        self._processor = processor_instance
        print(f'Running processor setup: {self._processor.__class__}')
        # NOTE: This is where the setup lifecycle method is called.
        self._processor.setup()
        self.process_time_gauge = Gauge(
            "process_time",
            description="Current process time of the actor. Goes up and down.",
            tag_keys=("actor_name", ),
        )
        self.process_time_gauge.set_default_tags(
            {"actor_name": self.__class__.__name__})

    def process(self, *args, **kwargs):
        return self._processor._process(*args, **kwargs)

    async def process_batch(self, calls: Iterable):
        start_time = time.time()
        to_ret = []
        for call in calls:
            to_ret.append(self.process(call))
        self.process_time_gauge.set(
            (time.time() - start_time) * 1000 / len(to_ret))
        return to_ret
