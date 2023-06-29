from buildflow.api_v2.node.infra.infra_api import InfraStatus
from buildflow.api_v2.node.state import StateAPI


class InfraState(StateAPI):
    status: InfraStatus
    timestamp_millis: int
