import dataclasses

from buildflow.config._config import Config
from buildflow.core import utils


@dataclasses.dataclass
class PulumiConfig(Config):
    stack_name: str
    project_name: str
    passphrase: str
    backend_url: str
    pulumi_home: str

    @classmethod
    def default(cls, pulumi_workspace_dir: str) -> "PulumiConfig":
        return cls(
            stack_name="buildflow-dev",
            project_name="buildflow-app",
            passphrase="buildflow-is-awesome",
            backend_url=f"file://{pulumi_workspace_dir}",
            pulumi_home=pulumi_workspace_dir,
        )

    @classmethod
    def load(cls, pulumi_config_path: str) -> "PulumiConfig":
        utils.assert_path_exists(pulumi_config_path)
        return cls(**utils.read_json_file(pulumi_config_path))

    def dump(self, pulumi_config_path: str):
        utils.write_json_file(pulumi_config_path, dataclasses.asdict(self))
