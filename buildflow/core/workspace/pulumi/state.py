import dataclasses

from buildflow import utils
from buildflow.api.shared import State


@dataclasses.dataclass
class PulumiState(State):
    @classmethod
    def initial(cls) -> "PulumiState":
        return cls()

    @classmethod
    def load(cls, pulumi_state_path: str) -> "PulumiState":
        utils.assert_path_exists(pulumi_state_path)
        return cls()

    def dump(self, pulumi_state_path: str):
        utils.write_json_file(pulumi_state_path, {})
