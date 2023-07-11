import dataclasses
import os
from typing import Optional

from buildflow.api.shared import State
from buildflow.core import utils
from buildflow.core.infra.state import InfraState
from buildflow.core.runtime.state import RuntimeState


@dataclasses.dataclass
class NodeState(State):
    runtime_state: Optional[RuntimeState]
    infra_state: Optional[InfraState]

    def set_runtime_state(self, runtime_state: RuntimeState):
        self.runtime_state = runtime_state

    def set_infra_state(self, infra_state: InfraState):
        self.infra_state = infra_state

    @classmethod
    def initial(cls) -> "NodeState":
        return NodeState(
            runtime_state=RuntimeState.initial(), infra_state=InfraState.initial()
        )

    @classmethod
    def load(cls, node_state_dir: str) -> "NodeState":
        utils.assert_path_exists(node_state_dir)
        # load the runtime state
        runtime_state_file_path = os.path.join(node_state_dir, "runtime.json")
        runtime_state = RuntimeState.load(runtime_state_file_path)
        # load the infra state
        infra_state_file_path = os.path.join(node_state_dir, "infra.json")
        infra_state = InfraState.load(infra_state_file_path)
        # return the node state
        return cls(runtime_state, infra_state)

    def dump(self, node_state_dir: str):
        # dump the runtime state (if it exists)
        if self.runtime_state is not None:
            runtime_state_file_path = os.path.join(node_state_dir, "runtime.json")
            self.runtime_state.dump(runtime_state_file_path)
        # dump the infra state (if it exists)
        if self.infra_state is not None:
            infra_state_file_path = os.path.join(node_state_dir, "infra.json")
            self.infra_state.dump(infra_state_file_path)
