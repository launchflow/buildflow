from buildflow.api_v2.node.runtime.runtime_api import RuntimeStatus
from buildflow.api_v2.node.state import StateAPI


class RuntimeState(StateAPI):
    status: RuntimeStatus
    timestamp_millis: int
