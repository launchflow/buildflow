import dataclasses
import os
from typing import Optional

from buildflow.core import utils
from buildflow.core.app.state._state import State
from buildflow.core.app.state.infra_state import InfraState
from buildflow.core.app.state.runtime_state import RuntimeState


@dataclasses.dataclass
class FlowState(State):
    runtime_state: Optional[RuntimeState]
    infra_state: Optional[InfraState]

    def set_runtime_state(self, runtime_state: RuntimeState):
        self.runtime_state = runtime_state

    def set_infra_state(self, infra_state: InfraState):
        self.infra_state = infra_state

    @classmethod
    def initial(cls) -> "FlowState":
        return FlowState(
            runtime_state=RuntimeState.initial(), infra_state=InfraState.initial()
        )

    @classmethod
    def load(cls, flow_state_dir: str) -> "FlowState":
        utils.assert_path_exists(flow_state_dir)
        # load the runtime state
        runtime_state_file_path = os.path.join(flow_state_dir, "runtime.json")
        runtime_state = RuntimeState.load(runtime_state_file_path)
        # load the infra state
        infra_state_file_path = os.path.join(flow_state_dir, "infra.json")
        infra_state = InfraState.load(infra_state_file_path)
        # return the flow state
        return cls(runtime_state, infra_state)

    def dump(self, flow_state_dir: str):
        # dump the runtime state (if it exists)
        if self.runtime_state is not None:
            runtime_state_file_path = os.path.join(flow_state_dir, "runtime.json")
            self.runtime_state.dump(runtime_state_file_path)
        # dump the infra state (if it exists)
        if self.infra_state is not None:
            infra_state_file_path = os.path.join(flow_state_dir, "infra.json")
            self.infra_state.dump(infra_state_file_path)
