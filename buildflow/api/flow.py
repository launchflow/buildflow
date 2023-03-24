from typing import Any, Optional

from buildflow.api import options
from buildflow.api.processor import ProcessorAPI


class FlowResults:

    def results(self):
        pass


class FlowAPI:

    def processor(input, output: Optional[Any] = None):
        pass

    def run(processor_instance: Optional[ProcessorAPI] = None,
            streaming_options: options.StreamingOptions = options.
            StreamingOptions()) -> FlowResults:
        pass
