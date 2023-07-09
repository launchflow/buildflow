import dataclasses
import os

from buildflow.config._config import Config
from buildflow.config.cloud_provider_config import CloudProviderConfig
from buildflow.config.pulumi_config import PulumiConfig
from buildflow.core import utils


@dataclasses.dataclass
class BuildFlowConfig(Config):
    pulumi_config: PulumiConfig
    cloud_provider_config: CloudProviderConfig

    @classmethod
    def default(cls, *, buildflow_config_dir: str) -> "BuildFlowConfig":
        pulumi_home_dir = os.path.join(buildflow_config_dir, "_pulumi")
        os.makedirs(pulumi_home_dir, exist_ok=True)
        return cls(
            pulumi_config=PulumiConfig.default(pulumi_home_dir=pulumi_home_dir),
            cloud_provider_config=CloudProviderConfig.default(),
        )

    @classmethod
    def create_or_load(cls, buildflow_config_dir: str) -> "BuildFlowConfig":
        if os.path.exists(buildflow_config_dir):
            return cls.load(buildflow_config_dir)
        else:
            config = cls.default(buildflow_config_dir=buildflow_config_dir)
            config.dump(buildflow_config_dir)
            return config

    @classmethod
    def load(cls, buildflow_config_dir: str) -> "BuildFlowConfig":
        utils.assert_path_exists(buildflow_config_dir)
        # load the pulumi config
        pulumi_config_dir = os.path.join(buildflow_config_dir, "pulumi_config.yaml")
        pulumi_config = PulumiConfig.load(pulumi_config_dir)
        # load the cloud provider config
        cloud_provider_config_dir = os.path.join(
            buildflow_config_dir, "cloud_provider_config.yaml"
        )
        cloud_provider_config = CloudProviderConfig.load(cloud_provider_config_dir)
        # return the buildflow config
        return cls(pulumi_config, cloud_provider_config)

    def dump(self, buildflow_config_dir: str):
        # dump the pulumi config
        pulumi_config_dir = os.path.join(buildflow_config_dir, "pulumi_config.yaml")
        self.pulumi_config.dump(pulumi_config_dir)
        # dump the cloud provider config
        cloud_provider_config_dir = os.path.join(
            buildflow_config_dir, "cloud_provider_config.yaml"
        )
        self.cloud_provider_config.dump(cloud_provider_config_dir)
