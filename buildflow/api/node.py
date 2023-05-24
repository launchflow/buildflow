from typing import Any, Optional

from buildflow.api import options
from buildflow.api.processor import ProcessorAPI


class NodeResults:
    def __init__(self, node_name: str) -> None:
        self.node_name = node_name

    async def output(self, register_shutdown: bool = True):
        """This method will block the flow until completion."""
        pass

    async def shutdown(self):
        """Sends the shutdown signal to the running flow."""
        pass


class NodeAPI:
    def __init__(self, name) -> None:
        self.name = name

    def processor(input, output: Optional[Any] = None):
        pass

    def add_processor(self, processor: ProcessorAPI):
        pass

    def run(
        *,
        streaming_options: options.StreamingOptions = options.StreamingOptions(),
        disable_usage_stats: bool = False,
        enable_resource_creation: bool = True,
        blocking: bool = True,
    ):
        pass
