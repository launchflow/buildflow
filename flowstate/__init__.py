# flake8: noqa
from typing import Optional

from flowstate.api import *
from flowstate.runtime.processor import Processor, processor
from flowstate.runtime.runner import Runtime


def run(processor_class: Optional[type] = None):
    runtime = Runtime()
    if processor_class is not None:
        runtime.register_processor(processor_class, processor_class._input(),
                                   processor_class._output())
    return runtime.run()