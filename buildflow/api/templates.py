import dataclasses
from enum import Enum
from typing import Any, Dict

from buildflow.api.io import IO, HTTPEndpoint, PubSub
from buildflow.api.processor import ProcessorAPI


class Template:
    """Super class for all template types."""
    _template_type: str
    
    @classmethod
    def from_config(cls, node_info: Dict[str, Any]):
        template_type = node_info['_template_type']
        return _TEMPLATE_MAPPING[template_type](**node_info)

    # This instance method defines the reference to the managed Processor.
    def processor(self) -> ProcessorAPI:
        raise NotImplementedError('processor not implemented')



class TemplateType(Enum):
    CloudRun = 'CLOUD_RUN'
    GCSFileEventStream = 'GCS_FILE_EVENT_STREAM'
    Cron = 'CRON'


@dataclasses.dataclass(frozen=True)
class CloudRun(Template):
    project_id: str
    public_access: bool
    endpoint: HTTPEndpoint
    
    _template_type: str = TemplateType.CloudRun.value


@dataclasses.dataclass(frozen=True)
class GCSFileEventStream(Template):
    glob_pattern: str
    pubsub: PubSub
    sink: IO
    
    _template_type: str = TemplateType.GCSFileEventStream.value


@dataclasses.dataclass(frozen=True)
class Cron(Template):
    schedule: str
    source: IO
    sink: IO
    
    _template_type: str = TemplateType.Cron.value


_TEMPLATE_MAPPING = {
    TemplateType.CloudRun.value: CloudRun,
    TemplateType.GCSFileEventStream.value: GCSFileEventStream,
    TemplateType.Cron.value: Cron,
}
