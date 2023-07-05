import os

from buildflow import utils
from buildflow.exceptions import PathNotFoundException
from buildflow.api.project import ProjectAPI, ProjectID
from buildflow.core.project.config import ProjectConfig


def _project_config_dir(project_dir: str) -> str:
    return os.path.join(project_dir, ".buildflow", "config")


# TODO: Deteremine if its worth adding a Meta type to the API (similar to State, but
# not managed by the Workspace)
# At the time of this comment, this is the only use case for a Meta type
def _project_metadata_path(project_dir: str) -> str:
    return os.path.join(project_dir, ".buildflow", "meta.json")


# NOTE: This is only used for the default project config, and then the project id is
# stored on disk in the project metadata
def _generate_project_id(project_dir: str) -> ProjectID:
    return utils.stable_hash(project_dir)[:8]


class Project(ProjectAPI):
    def __init__(self, project_id: ProjectID, project_config: ProjectConfig):
        self.project_id = project_id
        self.project_config = project_config

    def get_resource_config(self):
        return self.project_config.get_resource_config()

    @classmethod
    def create(cls, project_dir: str) -> "Project":
        project_id = _generate_project_id(project_dir)
        # dump the project metadata
        utils.write_json_file(
            _project_metadata_path(project_dir), {"project_id": project_id}
        )
        # create the default project config and dump it
        project_config = ProjectConfig.default()
        project_config.dump(_project_config_dir(project_dir))
        # return the project workspace
        return cls(project_id, project_config)

    @classmethod
    def load(cls, project_dir: str) -> "Project":
        utils.assert_path_exists(project_dir)
        # load the project metadata
        metadata_path = _project_metadata_path(project_dir)
        utils.assert_path_exists(metadata_path)
        project_metadata = utils.read_json_file(metadata_path)
        # load the project config
        project_config = ProjectConfig.load(_project_config_dir(project_dir))
        # return the project workspace
        return cls(project_metadata["project_id"], project_config)


_PROJECTS = {}


def get_or_create_project(project_dir: str) -> Project:
    if project_dir not in _PROJECTS:
        try:
            _PROJECTS[project_dir] = Project.load(project_dir)
        except PathNotFoundException:
            _PROJECTS[project_dir] = Project.create(project_dir)
    return _PROJECTS[project_dir]
