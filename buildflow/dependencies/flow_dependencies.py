import dataclasses

from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.core.credentials.gcp_credentials import GCPCredentials


@dataclasses.dataclass
class FlowCredentials:
    gcp_credentials: GCPCredentials
    aws_credentials: AWSCredentials
