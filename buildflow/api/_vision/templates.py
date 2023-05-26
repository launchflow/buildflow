import dataclasses

from buildflow.api.io import IO, HTTPEndpoint, PubSub
from buildflow.api.processor import ProcessorAPI, NodeAPI


class NodeTemplate:
    """Super class for all template types."""

    # This instance method defines the reference to the managed Processor.
    def node(self) -> NodeAPI:
        raise NotImplementedError("processor not implemented")


@dataclasses.dataclass(frozen=True)
class CloudRun(Template):
    project_id: str
    public_access: bool
    endpoint: HTTPEndpoint


@dataclasses.dataclass(frozen=True)
class GCSFileEventStream(Template):
    glob_pattern: str
    pubsub: PubSub
    sink: IO


@dataclasses.dataclass(frozen=True)
class CloudScheduler(Template):
    cron_schedule: str
    source: IO
    sink: IO
