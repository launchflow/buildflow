import copy
import dataclasses
from typing import Iterable

import ray

from buildflow.api import SinkType, SourceType, ProcessorAPI


@dataclasses.dataclass
class ProcessorRef:
    processor_instance: ProcessorAPI
    source: SourceType
    sink: SinkType

    def get_processor_replica(self):
        return copy.deepcopy(self.processor_instance)


# TODO(#113): make this configurable by the user
@ray.remote(num_cpus=.5)
class ProcessActor(object):

    def __init__(self, processor_instance: ProcessorAPI):
        self._processor = processor_instance
        print(f'Running processor setup: {self._processor.__class__}')
        # NOTE: This is where the setup lifecycle method is called.
        self._processor.setup()

    def process(self, *args, **kwargs):
        return self._processor._process(*args, **kwargs)

    def process_batch(self, calls: Iterable):
        to_ret = []
        for call in calls:
            to_ret.append(self.process(call))
        return to_ret
