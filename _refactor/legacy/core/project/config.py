import dataclasses
import os

from buildflow.api.shared import Config
from buildflow.core import utils
from buildflow.resources.config import ResourcesConfig


def _resources_config_dir(project_config_dir: str) -> str:
    return os.path.join(project_config_dir, "resources")


@dataclasses.dataclass
class ProjectConfig(Config):
    resource_config: ResourcesConfig

    def get_resource_config(self) -> ResourcesConfig:
        return self.resource_config

    @classmethod
    def default(cls) -> "ProjectConfig":
        return cls(resource_config=ResourcesConfig.default())

    @classmethod
    def load(cls, project_config_dir: str) -> "ProjectConfig":
        utils.assert_path_exists(project_config_dir)
        # load the resource config
        resource_config = ResourcesConfig.load(
            _resources_config_dir(project_config_dir)
        )
        # return the project config
        return cls(resource_config)

    def dump(self, project_config_dir: str):
        # dump the resource config
        self.resource_config.dump(_resources_config_dir(project_config_dir))
