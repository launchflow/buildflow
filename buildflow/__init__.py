# flake8: noqa
from typing import Optional

from buildflow.api import *
from buildflow.runtime.processor import Processor, processor
from buildflow.runtime.runner import Runtime


def run(processor_class: Optional[type] = None, num_replicas: int = 1):
    runtime = Runtime()
    if processor_class is not None:
        runtime.register_processor(processor_class, processor_class._input(),
                                   processor_class._output())
    return runtime.run(num_replicas)