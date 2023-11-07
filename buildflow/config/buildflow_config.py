import dataclasses
import os
from typing import List, Optional

import dacite

from buildflow.config._config import Config
from buildflow.config.cloud_provider_config import CloudProviderConfig
from buildflow.config.pulumi_config import PulumiConfig
from buildflow.core import utils

BUILDFLOW_CONFIG_FILE = "buildflow.yaml"


@dataclasses.dataclass
class BuildFlowConfig(Config):
    app: str
    pulumi_config: PulumiConfig
    entry_point: str
    cloud_provider_config: Optional[CloudProviderConfig]
    build_ignores: List[str] = dataclasses.field(default_factory=list)

    @classmethod
    def default(cls, *, directory: str, app: str) -> "BuildFlowConfig":
        return cls(
            app=app,
            pulumi_config=PulumiConfig.default(
                project_name=app,
                directory=directory,
            ),
            entry_point="main:app",
            cloud_provider_config=None,
        )

    @classmethod
    def create(cls, directory: str, app: str) -> "BuildFlowConfig":
        buildflow_config_dir = os.path.join(directory, BUILDFLOW_CONFIG_FILE)
        if os.path.exists(buildflow_config_dir):
            raise FileExistsError(
                f"BuildFlow config file already exists at {buildflow_config_dir}"
            )
        config = cls.default(
            app=app,
            directory=directory,
        )
        config.dump(buildflow_config_dir)
        return config

    @classmethod
    def load(cls, directory: str = os.getcwd()) -> "BuildFlowConfig":
        buildflow_config_dir = os.path.join(directory, BUILDFLOW_CONFIG_FILE)
        utils.assert_path_exists(buildflow_config_dir)
        config_dict = utils.read_yaml_file(buildflow_config_dir)
        app = config_dict["app"]
        pulumi_config_dict = config_dict["pulumi_config"]
        pulumi_config_dict["project_name"] = app
        pulumi_config = dacite.from_dict(
            data=pulumi_config_dict, data_class=PulumiConfig
        )
        pulumi_config.load()
        cloud_provider_config = None
        if "cloud_provider_config" in config_dict:
            cloud_provider_config = CloudProviderConfig.fromdict(
                config_dict["cloud_provider_config"]
            )
        return cls(
            app=app,
            pulumi_config=pulumi_config,
            cloud_provider_config=cloud_provider_config,
            entry_point=config_dict["entry_point"],
            build_ignores=config_dict.get("build_ignores", []),
        )

    def dump(self, directory: str):
        buildflow_config_dir = os.path.join(directory, BUILDFLOW_CONFIG_FILE)
        pulumi_dict = self.pulumi_config.asdict()
        config_dict = {
            "app": self.app,
            "pulumi_config": pulumi_dict,
            "entry_point": self.entry_point,
            "build_ignores": self.build_ignores,
        }
        if self.cloud_provider_config is not None:
            cloud_provider_dict = self.cloud_provider_config.asdict()
            config_dict["cloud_provider_config"] = cloud_provider_dict
        utils.write_yaml_file(buildflow_config_dir, config_dict)
