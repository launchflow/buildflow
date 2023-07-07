import enum


class StategyType(enum.Enum):
    ENDPOINT = "endpoint"
    SINK = "sink"
    SOCKET = "socket"
    SOURCE = "source"


StrategyID = str


class Strategy:
    strategy_type: StategyType

    def __init__(self, strategy_id: StrategyID):
        self.strategy_id = strategy_id
