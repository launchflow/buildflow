import enum

from buildflow.core.credentials import CredentialType


class StategyType(enum.Enum):
    ENDPOINT = "endpoint"
    SINK = "sink"
    SOCKET = "socket"
    SOURCE = "source"


StrategyID = str


class Strategy:
    strategy_type: StategyType

    def __init__(self, credentials: CredentialType, strategy_id: StrategyID):
        self.credentials = credentials
        self.strategy_id = strategy_id
