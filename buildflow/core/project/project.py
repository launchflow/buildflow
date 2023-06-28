import os

from buildflow import utils
from buildflow.exceptions import PathNotFoundException
from buildflow.api.project import ProjectAPI, ProjectID
from buildflow.core.project.config import ProjectConfig


def _project_config_dir(project_dir: str) -> str:
    return os.path.join(project_dir, ".buildflow", "config")


def _get_project_id(project_dir: str) -> ProjectID:
    return utils.stable_hash(project_dir)[:8]


class Project(ProjectAPI):
    def __init__(self, project_id: ProjectID, project_config: ProjectConfig):
        self.project_id = project_id
        self.project_config = project_config

    def get_resource_config(self):
        return self.project_config.get_resource_config()

    @classmethod
    def create(cls, project_dir: str) -> "Project":
        project_id = _get_project_id(project_dir)
        # create the default project config
        project_config = ProjectConfig.default()
        project_config.dump(_project_config_dir(project_dir))
        # return the project workspace
        return cls(project_id, project_config)

    @classmethod
    def load(cls, project_dir: str) -> "Project":
        utils.assert_path_exists(project_dir)
        project_id = _get_project_id(project_dir)
        # load the project config
        project_config = ProjectConfig.load(_project_config_dir(project_dir))
        # return the project workspace
        return cls(project_id, project_config)


_PROJECTS = {}


def get_or_create_project(project_dir: str) -> Project:
    if project_dir not in _PROJECTS:
        try:
            _PROJECTS[project_dir] = Project.load(project_dir)
        except PathNotFoundException:
            _PROJECTS[project_dir] = Project.create(project_dir)
    return _PROJECTS[project_dir]
