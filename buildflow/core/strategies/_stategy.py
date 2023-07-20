import enum

from buildflow.core.options.runtime_options import RuntimeOptions


class StategyType(enum.Enum):
    ENDPOINT = "endpoint"
    SINK = "sink"
    SOCKET = "socket"
    SOURCE = "source"


StrategyID = str


class Strategy:
    strategy_type: StategyType

    def __init__(self, runtime_options: RuntimeOptions, strategy_id: StrategyID):
        self.runtime_options = runtime_options
        self.strategy_id = strategy_id
