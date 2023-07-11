import dataclasses

from buildflow.api import RuntimeStatus, State
from buildflow.core import utils


@dataclasses.dataclass
class RuntimeState(State):
    status: RuntimeStatus
    timestamp_millis: int

    @classmethod
    def initial(cls) -> None:
        return None

    @classmethod
    def load(cls, runtime_state_file_path: str) -> "RuntimeState":
        utils.assert_path_exists(runtime_state_file_path)
        runtime_state_dict = utils.read_json_file(runtime_state_file_path)
        return cls(
            status=RuntimeStatus[runtime_state_dict["status"]],
            timestamp_millis=runtime_state_dict["timestamp_millis"],
        )

    def dump(self, runtime_state_file_path: str):
        runtime_state_dict = {
            "status": self.status.name,
            "timestamp_millis": self.timestamp_millis,
        }
        utils.write_json_file(runtime_state_file_path, runtime_state_dict)
