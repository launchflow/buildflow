import dataclasses
import os

from buildflow.config._config import Config
from buildflow.config.flow_config import FlowConfig
from buildflow.config.registry_config import RegistryConfig
from buildflow.core import utils


@dataclasses.dataclass
class BuildFlowConfig(Config):
    flow_config: FlowConfig
    registry_config: RegistryConfig

    @classmethod
    def default(cls) -> "BuildFlowConfig":
        return cls(
            flow_config=FlowConfig.default(), registry_config=RegistryConfig.default()
        )

    @classmethod
    def load(cls, buildflow_config_dir: str) -> "BuildFlowConfig":
        utils.assert_path_exists(buildflow_config_dir)
        # load the flow config
        flow_config_dir = os.path.join(buildflow_config_dir, "flow")
        flow_config = FlowConfig.load(flow_config_dir)
        # load the registry config
        registry_config_dir = os.path.join(buildflow_config_dir, "registry")
        registry_config = RegistryConfig.load(registry_config_dir)
        # return the buildflow config
        return cls(flow_config, registry_config)

    def dump(self, buildflow_config_dir: str):
        # dump the flow config
        flow_config_dir = os.path.join(buildflow_config_dir, "flow")
        self.flow_config.dump(flow_config_dir)
        # dump the registry config
        registry_config_dir = os.path.join(buildflow_config_dir, "registry")
        self.registry_config.dump(registry_config_dir)
