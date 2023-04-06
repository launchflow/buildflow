from typing import Any, Optional

from buildflow.api import options
from buildflow.api.processor import ProcessorAPI


class FlowResults:

    def output(self):
        """This method will block the flow until completion.

        For batch flows it will return the output of the pipeline.

        For streaming flows it will simply infinitely block.
        """
        pass

    def shutdown(self):
        """Sends the shutdown signal to the running flow."""
        pass


class FlowAPI:

    def processor(input, output: Optional[Any] = None):
        pass

    def run(
        processor_instance: Optional[ProcessorAPI] = None,
        *,
        streaming_options: options.StreamingOptions = options.StreamingOptions(
        ),
        disable_usage_stats: bool = False,
        enable_resource_creation: bool = True,
    ) -> FlowResults:
        pass
